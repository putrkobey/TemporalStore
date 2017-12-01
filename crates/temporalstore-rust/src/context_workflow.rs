// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Max ref_hashes per ContextQueryEmbeddings command. Must not exceed the
/// engine's CONTEXT_MAX_LIMIT (engine/engine_constants.rs), which command validation
/// enforces by rejecting larger requests. Retrieval chunks its summary-embedding
/// lookups at this size so a namespace with more nodes than the cap is still
/// fully scored instead of silently falling back to lexical ranking.
const CONTEXT_EMBEDDING_QUERY_CHUNK: usize = 1000;

/// Reason code stamped on an embedding-dirty marker written by the live extraction
/// path when the inline embedding call fails (after any fallback provider). Purely
/// diagnostic; the drainer treats every pending marker identically. Distinct from
/// the bulk-ingest deferral reason (2, see context_batch_ingest.rs).
const EMBEDDING_DIRTY_REASON_LIVE_FAILURE: u32 = 3;

/// Raw lexical-relevance score at/above which a node is treated as a full-strength
/// lexical match, mapped to `LEXICAL_MATCH_MICROS`. Scores below scale linearly.
/// A topic-phrase hit alone already contributes 1000 in `context_relevance_score_plan`.
const LEXICAL_SCORE_SATURATION: i64 = 1_000;
/// Micros a saturated lexical match maps to. Kept below the 1_000_000 ceiling a
/// perfect cosine match reaches so that, in a MIXED store, a strong semantic
/// (embedded) match still outranks a purely lexical one, while un-embedded nodes
/// remain rankable (never a flat 0) instead of collapsing to recency order.
const LEXICAL_MATCH_MICROS: i64 = 500_000;


/// Map a raw lexical-relevance score into the same micros scale cosine similarity
/// uses (`context_embedding_similarity_micros`), so lexical and cosine node scores
/// merge into one ranking. 0 stays 0 (no overlap => not surfaced by lexical).
fn lexical_score_to_micros(raw: u32) -> i64 {
    let raw = (raw as i64).min(LEXICAL_SCORE_SATURATION);
    raw.saturating_mul(LEXICAL_MATCH_MICROS) / LEXICAL_SCORE_SATURATION
}

#[path = "context_workflow/context_query_scoring.rs"]
mod query;
mod embed_drainer;
#[path = "context_workflow/context_workflow_resource.rs"]
mod resource;
#[path = "context_workflow/context_workflow_benchmark.rs"]
mod benchmark;
mod model_provider;
#[path = "context_workflow/context_workflow_ingest.rs"]
mod ingest;
#[path = "context_workflow/context_workflow_reports.rs"]
mod reports;
#[path = "context_workflow/context_workflow_skill.rs"]
mod skill;

pub use resource::{
    parse_context_resource, update_context_resource_lifecycle,
};
pub use benchmark::{run_context_pipeline_benchmark, run_context_pipeline_benchmark_sweep};
pub(crate) use benchmark::*;
pub use reports::*;
pub use ingest::{
    ingest_extract_context, ingest_extract_context_with_l1_gate, ingest_resource_skill_context,
    validate_resource_skill_secondary_indexes,
};
pub(crate) use model_provider::*;
pub use model_provider::{context_backfill_embeddings, context_provider_from_env};
pub use query::context_embedding_ref_hash;
pub use embed_drainer::{
    drain_embedding_dirty_once, embed_drainer_config_from_env, embed_drainer_enabled,
    run_embed_drainer_loop, EmbedDrainReport, EmbedDrainerConfig,
};
pub use skill::{
    context_skill_registry_from_parsed, parse_context_skill_markdown,
    select_context_skills_for_retrieval, update_context_skill_registry,
};

use self::query::*;
use self::resource::{
    context_resource_lifecycle_report, default_resource_max_chunk_chars,
    default_resource_overlap_chars, default_resource_parser_name, default_resource_parser_version,
};

use crate::engine::TemporalEngine;
use crate::http::{post_json_with_options_and_headers, HttpRequestOptions};
use crate::types::{
    context_model_descriptors, context_node_summary_vector_enabled, Command, CommandResponse,
    ContextAuditRef, ContextChildRef, CONTEXT_SUMMARY_LEVEL_L1,
    ContextCompressionEvent, ContextEntity, ContextEvent, ContextIndexRef,
    ContextModelDescriptor, ContextNode, ContextPackAudit, ContextSummary,
    ContextDirtyNode, ExecuteRequest, ShardId, Status,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextTier {
    L0,
    L1,
    L2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextSourceKind {
    Document,
    Chat,
    Ticket,
    Code,
    Incident,
    UserEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextProviderKind {
    Mock,
    OpenAiCompatible,
}

impl Default for ContextProviderKind {
    fn default() -> Self {
        Self::Mock
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextModelProviderConfig {
    #[serde(default = "default_provider_name")]
    pub provider_name: String,
    #[serde(default)]
    pub provider_kind: ContextProviderKind,
    #[serde(default)]
    pub base_url: String,
    #[serde(default)]
    pub api_key_env: String,
    #[serde(default = "default_chat_model")]
    pub model: String,
    #[serde(default = "default_embedding_model")]
    pub embedding_model: String,
    #[serde(default = "default_vlm_model")]
    pub vlm_model: String,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,
    #[serde(default)]
    pub fallback_provider: Option<Box<ContextModelProviderConfig>>,
    #[serde(default = "default_true")]
    pub mock_mode: bool,
}

impl Default for ContextModelProviderConfig {
    fn default() -> Self {
        Self {
            provider_name: default_provider_name(),
            provider_kind: ContextProviderKind::Mock,
            base_url: String::new(),
            api_key_env: String::new(),
            model: default_chat_model(),
            embedding_model: default_embedding_model(),
            vlm_model: default_vlm_model(),
            timeout_ms: default_timeout_ms(),
            max_retries: default_max_retries(),
            fallback_provider: None,
            mock_mode: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextExtractRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    pub source_kind: ContextSourceKind,
    pub source_id: String,
    pub title: String,
    pub body: String,
    pub timestamp_ms: u64,
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextEmbeddingGenerationReport {
    pub status: Status,
    pub provider_name: String,
    pub provider_kind: ContextProviderKind,
    pub embedding_model: String,
    pub vector_dimension: usize,
    pub requested_vector_count: usize,
    pub generated_vector_count: usize,
    pub batch_count: usize,
    pub live_call_count: usize,
    pub fallback_used: bool,
    pub mock_mode: bool,
    pub production_evidence_ready: bool,
}

impl Default for ContextEmbeddingGenerationReport {
    fn default() -> Self {
        Self {
            status: Status::ok(),
            provider_name: default_provider_name(),
            provider_kind: ContextProviderKind::Mock,
            embedding_model: default_embedding_model(),
            vector_dimension: 0,
            requested_vector_count: 0,
            generated_vector_count: 0,
            batch_count: 0,
            live_call_count: 0,
            fallback_used: false,
            mock_mode: true,
            production_evidence_ready: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextExtractReport {
    pub status: Status,
    pub provider: ContextModelProviderConfig,
    #[serde(default)]
    pub embedding_generation: ContextEmbeddingGenerationReport,
    pub node: ContextNode,
    pub event: ContextEvent,
    pub index_ref: ContextIndexRef,
    pub dirty_marker: ContextDirtyNode,
    #[serde(default)]
    pub source_ref: String,
    #[serde(default)]
    pub related_node_hashes: Vec<u64>,
    #[serde(default)]
    pub summary_refs: Vec<String>,
    #[serde(default)]
    pub compact_summary_ref: String,
    pub node_uri: String,
    pub event_uri: String,
    pub l0: String,
    pub l1: String,
    pub l2_ref: String,
}

pub fn default_context_resource_max_inline_bytes() -> usize {
    1 * 1024 * 1024
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceParseRequest {
    pub raw_uri: String,
    #[serde(default)]
    pub resource_type: Option<String>,
    pub text: String,
    #[serde(default = "default_resource_max_chunk_chars")]
    pub max_chunk_chars: usize,
    #[serde(default = "default_resource_overlap_chars")]
    pub overlap_chars: usize,
    #[serde(default)]
    pub chunk_hash_base: Option<u64>,
    #[serde(default)]
    pub owner_scope: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub watch_interval_minutes: u64,
    #[serde(default)]
    pub parser_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextScopeDescriptor {
    pub raw_scope: String,
    pub layer: String,
    pub owner_id: String,
    pub shared_graph_scope: String,
    pub producer_agent_id: String,
    pub precedence_rank: u32,
}

fn context_scope_descriptor(owner_scope: &str) -> ContextScopeDescriptor {
    let raw_scope = owner_scope.trim();
    let normalized = if raw_scope.is_empty() {
        "user".to_string()
    } else {
        raw_scope.to_ascii_lowercase()
    };
    let (layer, owner_id) = normalized
        .split_once(':')
        .or_else(|| normalized.split_once('/'))
        .map(|(layer, owner_id)| (layer.to_string(), owner_id.to_string()))
        .unwrap_or_else(|| (normalized.clone(), String::new()));
    let layer = match layer.as_str() {
        "global" | "workspace" | "user" | "agent" | "resource" | "skill" => layer,
        _ => "user".to_string(),
    };
    let shared_graph_scope = match layer.as_str() {
        "global" => "global".to_string(),
        "workspace" => format!("workspace:{owner_id}"),
        "agent" => {
            if owner_id.is_empty() {
                "user".to_string()
            } else {
                format!("agent:{owner_id}")
            }
        }
        "resource" | "skill" => format!("{layer}:{owner_id}"),
        _ => {
            if owner_id.is_empty() {
                "user".to_string()
            } else {
                format!("user:{owner_id}")
            }
        }
    };
    let precedence_rank = match layer.as_str() {
        "agent" => 0,
        "user" => 10,
        "workspace" => 20,
        "resource" | "skill" => 30,
        "global" => 40,
        _ => 50,
    };
    ContextScopeDescriptor {
        raw_scope: raw_scope.to_string(),
        layer,
        owner_id: owner_id.clone(),
        shared_graph_scope,
        producer_agent_id: if normalized.starts_with("agent") {
            owner_id
        } else {
            String::new()
        },
        precedence_rank,
    }
}

fn context_scope_layer_name(layer: impl AsRef<str>) -> &'static str {
    match layer.as_ref() {
        "agent" => "agent",
        "user" => "user",
        "workspace" => "workspace",
        "resource" => "resource",
        "skill" => "skill",
        "global" => "global",
        _ => "user",
    }
}

fn context_scope_matches(
    requested: &ContextScopeDescriptor,
    candidate: &ContextScopeDescriptor,
) -> bool {
    candidate.layer == "global"
        || requested
            .raw_scope
            .eq_ignore_ascii_case(&candidate.raw_scope)
        || requested.shared_graph_scope == candidate.shared_graph_scope
        || (requested.layer == "agent" && matches!(candidate.layer.as_str(), "user" | "workspace"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ContextResourceLifecycleAction {
    #[default]
    Add,
    Watch,
    Refresh,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ContextResourceImportKind {
    #[default]
    Text,
    Markdown,
    Skill,
    Url,
    GitRepo,
    CodeRepo,
    Pdf,
    Document,
    WikiDoc,
    WatchedResource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceLifecycleRecord {
    pub raw_uri: String,
    pub target_uri: String,
    pub owner_scope: String,
    #[serde(default)]
    pub scope: ContextScopeDescriptor,
    pub parser_name: String,
    pub parser_version: String,
    pub resource_type: String,
    pub import_kind: ContextResourceImportKind,
    pub action: ContextResourceLifecycleAction,
    pub version: String,
    pub content_hash: u64,
    pub stale: bool,
    pub invalidates_version: String,
    pub watched: bool,
    pub watch_interval_minutes: u64,
    pub next_refresh_after_ms: u64,
    pub deleted: bool,
    pub chunk_count: usize,
    #[serde(default)]
    pub payload_size_bytes: usize,
    #[serde(default)]
    pub max_inline_bytes: usize,
    /// Whether the payload is held in the record rather than in the object store.
    ///
    /// Defaulted explicitly rather than with a bare `#[serde(default)]`: on a bool that decodes an
    /// ABSENT field to `false`, which here means "the payload is elsewhere" and sends a reader to
    /// an `external_object_uri` that such a record does not carry. This type's own `Default` says
    /// true, and decoding it should not disagree with constructing it.
    #[serde(default = "inline_payload_default")]
    pub inline_payload: bool,
    #[serde(default)]
    pub external_object_uri: String,
}

/// A record with nothing said about where its payload lives holds it inline; see the field.
fn inline_payload_default() -> bool {
    true
}

impl Default for ContextResourceLifecycleRecord {
    fn default() -> Self {
        Self {
            raw_uri: String::new(),
            target_uri: String::new(),
            owner_scope: "user".to_string(),
            scope: context_scope_descriptor("user"),
            parser_name: default_resource_parser_name(),
            parser_version: default_resource_parser_version(),
            resource_type: "txt".to_string(),
            import_kind: ContextResourceImportKind::Text,
            action: ContextResourceLifecycleAction::Add,
            version: String::new(),
            content_hash: 0,
            stale: false,
            invalidates_version: String::new(),
            watched: false,
            watch_interval_minutes: 0,
            next_refresh_after_ms: 0,
            deleted: false,
            chunk_count: 0,
            payload_size_bytes: 0,
            max_inline_bytes: default_context_resource_max_inline_bytes(),
            inline_payload: true,
            external_object_uri: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceLifecycleUpdate {
    pub raw_uri: String,
    pub action: ContextResourceLifecycleAction,
    #[serde(default)]
    pub owner_scope: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub watch_interval_minutes: u64,
    #[serde(default)]
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceLifecycleReport {
    pub status: Status,
    pub resources: Vec<ContextResourceLifecycleRecord>,
    pub watched_count: usize,
    pub stale_count: usize,
    pub deleted_count: usize,
    pub refresh_due_count: usize,
    pub import_kinds: BTreeMap<String, usize>,
}

impl Default for ContextResourceLifecycleReport {
    fn default() -> Self {
        Self {
            status: Status::ok(),
            resources: Vec::new(),
            watched_count: 0,
            stale_count: 0,
            deleted_count: 0,
            refresh_due_count: 0,
            import_kinds: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextParsedResourceChunk {
    pub chunk_hash: u64,
    #[serde(default)]
    pub content_hash: u64,
    pub embedding_ref_hash: u64,
    pub source_ref: String,
    #[serde(default)]
    pub parent_source_ref: Option<String>,
    #[serde(default)]
    pub heading_path: Vec<String>,
    pub text: String,
    pub token_estimate: u32,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceParseReport {
    pub status: Status,
    pub raw_uri: String,
    pub resource_type: String,
    pub resource_hash: u64,
    #[serde(default)]
    pub uri_scheme: String,
    #[serde(default)]
    pub resource_title: String,
    pub embedding_model: String,
    #[serde(default)]
    pub lifecycle: ContextResourceLifecycleRecord,
    pub chunks: Vec<ContextParsedResourceChunk>,
    #[serde(default)]
    pub source_refs: Vec<String>,
    pub total_tokens: u32,
    #[serde(default)]
    pub payload_size_bytes: usize,
    #[serde(default)]
    pub max_inline_bytes: usize,
    #[serde(default)]
    pub inline_payload: bool,
    #[serde(default)]
    pub external_object_uri: String,
    #[serde(default)]
    pub parser_warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillParseReport {
    pub status: Status,
    pub skill_name: String,
    pub description: String,
    pub source_ref: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub owner_scope: String,
    #[serde(default)]
    pub scope: ContextScopeDescriptor,
    #[serde(default = "default_context_skill_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub precedence: ContextSkillPrecedence,
    pub front_matter: BTreeMap<String, String>,
    pub tag_refs: Vec<String>,
    pub capability_refs: Vec<String>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub triggers: Vec<String>,
    #[serde(default)]
    pub model_refs: Vec<String>,
    pub tool_refs: Vec<String>,
    pub instruction_refs: Vec<String>,
    pub resource_refs: Vec<String>,
    pub example_refs: Vec<String>,
    #[serde(default)]
    pub parser_warnings: Vec<String>,
    pub resource: ContextResourceParseReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum ContextSkillPrecedence {
    Low,
    #[default]
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillRegistryEntry {
    pub skill_name: String,
    pub source_ref: String,
    pub description: String,
    pub version: String,
    pub owner_scope: String,
    #[serde(default)]
    pub scope: ContextScopeDescriptor,
    pub enabled: bool,
    pub precedence: ContextSkillPrecedence,
    pub triggers: Vec<String>,
    pub allowed_tools: Vec<String>,
    pub tag_refs: Vec<String>,
    pub model_refs: Vec<String>,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillRegistryUpdate {
    pub skill_name: String,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub precedence: Option<ContextSkillPrecedence>,
    #[serde(default)]
    pub owner_scope: Option<String>,
    #[serde(default)]
    pub triggers: Option<Vec<String>>,
    #[serde(default)]
    pub allowed_tools: Option<Vec<String>>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillRegistryReport {
    pub status: Status,
    pub entries: Vec<ContextSkillRegistryEntry>,
    pub enabled_count: usize,
    pub disabled_count: usize,
    pub highest_precedence: ContextSkillPrecedence,
    pub version_updates: Vec<String>,
    #[serde(default)]
    pub scope_layers: BTreeMap<String, usize>,
    #[serde(default)]
    pub shared_graph_scope_count: usize,
    #[serde(default)]
    pub producer_agent_count: usize,
}

impl Default for ContextSkillRegistryReport {
    fn default() -> Self {
        Self {
            status: Status::ok(),
            entries: Vec::new(),
            enabled_count: 0,
            disabled_count: 0,
            highest_precedence: ContextSkillPrecedence::Normal,
            version_updates: Vec::new(),
            scope_layers: BTreeMap::new(),
            shared_graph_scope_count: 0,
            producer_agent_count: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillSelectionRequest {
    pub query: String,
    #[serde(default)]
    pub owner_scope: String,
    #[serde(default)]
    pub tool_name: String,
    #[serde(default)]
    pub include_disabled: bool,
    #[serde(default)]
    pub allowed_scope_layers: Vec<String>,
    #[serde(default = "default_skill_selection_limit")]
    pub limit: usize,
    pub registry: Vec<ContextSkillRegistryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillSelectionCandidate {
    pub skill_name: String,
    pub version: String,
    pub owner_scope: String,
    #[serde(default)]
    pub scope: ContextScopeDescriptor,
    pub precedence: ContextSkillPrecedence,
    pub score: i64,
    pub matched_triggers: Vec<String>,
    pub allowed_tool_match: bool,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillSelectionReport {
    pub status: Status,
    pub query: String,
    pub selected: Vec<ContextSkillSelectionCandidate>,
    pub skipped_disabled: Vec<String>,
    pub skipped_owner_scope: Vec<String>,
    pub skipped_tool: Vec<String>,
    #[serde(default)]
    pub scope_resolution_order: Vec<String>,
    #[serde(default)]
    pub agent_producers: Vec<String>,
    #[serde(default)]
    pub shared_graph_scope_count: usize,
}

impl Default for ContextSkillSelectionReport {
    fn default() -> Self {
        Self {
            status: Status::ok(),
            query: String::new(),
            selected: Vec::new(),
            skipped_disabled: Vec::new(),
            skipped_owner_scope: Vec::new(),
            skipped_tool: Vec::new(),
            scope_resolution_order: Vec::new(),
            agent_producers: Vec::new(),
            shared_graph_scope_count: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillIngestInput {
    pub raw_uri: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextResourceSkillIngestRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    #[serde(default)]
    pub resources: Vec<ContextResourceParseRequest>,
    #[serde(default)]
    pub skills: Vec<ContextSkillIngestInput>,
    #[serde(default)]
    pub query: String,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    #[serde(default = "default_retrieve_limit")]
    pub max_events: usize,
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextResourceSkillModelFanoutReport {
    pub node_count: usize,
    pub event_count: usize,
    #[serde(alias = "segment_count")]
    pub slab_count: usize,
    pub entity_count: usize,
    pub child_ref_count: usize,
    pub embedding_count: usize,
    pub summary_count: usize,
    pub compression_count: usize,
    pub dirty_marker_count: usize,
    pub secondary_index_count: usize,
    pub query_back_ok: bool,
    pub missing_models: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextResourceSkillSecondaryIndexReport {
    pub resource_refs: Vec<String>,
    pub skill_refs: Vec<String>,
    pub entity_refs: Vec<String>,
    pub source_refs: Vec<String>,
    pub summary_refs: Vec<String>,
    pub query_back_ok: bool,
    pub missing_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextResourceSkillEmbeddingEvidenceReport {
    pub extract_count: usize,
    pub requested_vector_count: usize,
    pub generated_vector_count: usize,
    pub live_call_count: usize,
    pub mock_generation_count: usize,
    pub fallback_generation_count: usize,
    pub production_evidence_ready: bool,
    pub provider_names: Vec<String>,
    pub embedding_models: Vec<String>,
    pub vector_dimensions: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceSkillSecondaryIndexValidationRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    pub secondary_indexes: ContextResourceSkillSecondaryIndexReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSecondaryIndexFamilyValidationReport {
    pub index_name: String,
    pub checked_ref_count: usize,
    pub found_ref_count: usize,
    pub missing_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceSkillSecondaryIndexValidationReport {
    pub status: Status,
    pub query_back_ok: bool,
    pub checked_ref_count: usize,
    pub found_ref_count: usize,
    pub missing_refs: Vec<String>,
    pub families: Vec<ContextSecondaryIndexFamilyValidationReport>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextResourceSkillIngestReport {
    pub status: Status,
    pub resources: Vec<ContextResourceParseReport>,
    #[serde(default)]
    pub resource_lifecycle: ContextResourceLifecycleReport,
    pub skills: Vec<ContextSkillParseReport>,
    #[serde(default)]
    pub skill_registry: ContextSkillRegistryReport,
    #[serde(default)]
    pub skill_selection: ContextSkillSelectionReport,
    pub ingest: ContextIngestExtractReport,
    #[serde(default)]
    pub embedding_evidence: ContextResourceSkillEmbeddingEvidenceReport,
    pub fanout: ContextResourceSkillModelFanoutReport,
    pub secondary_indexes: ContextResourceSkillSecondaryIndexReport,
    pub retrieval: ContextRetrieveReport,
    pub parity: ContextPipelineParityEvidence,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextIngestExtractRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    pub sources: Vec<ContextExtractRequest>,
    #[serde(default)]
    pub query: String,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    #[serde(default = "default_retrieve_limit")]
    pub max_events: usize,
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextIngestExtractReport {
    pub status: Status,
    pub accepted: usize,
    pub failed: usize,
    pub summary: ContextIngestExtractSummary,
    pub extracts: Vec<ContextExtractReport>,
    pub failed_sources: Vec<ContextIngestSourceFailure>,
    pub node_hashes: Vec<u64>,
    pub retrieve_request: ContextRetrieveRequest,
    pub parity: ContextPipelineParityEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextIngestExtractSummary {
    pub source_count: usize,
    pub accepted: usize,
    pub failed: usize,
    pub unique_node_count: usize,
    #[serde(default)]
    pub extracted_node_count: usize,
    #[serde(default)]
    pub extracted_event_count: usize,
    #[serde(default)]
    pub extracted_index_ref_count: usize,
    #[serde(default)]
    pub extracted_dirty_marker_count: usize,
    #[serde(default)]
    pub extracted_summary_ref_count: usize,
    pub retrieval_node_count: usize,
    pub source_kind_counts: BTreeMap<String, usize>,
    pub provider_counts: BTreeMap<String, usize>,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextIngestSourceFailure {
    pub source_id: String,
    pub status: Status,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    #[serde(default = "default_benchmark_profile")]
    pub profile: String,
    #[serde(default = "default_benchmark_source_count")]
    pub source_count: usize,
    #[serde(default = "default_benchmark_query_count")]
    pub query_count: usize,
    #[serde(default = "default_retrieve_limit")]
    pub max_events: usize,
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
    #[serde(default = "default_benchmark_thresholds")]
    pub thresholds: ContextPipelineBenchmarkThresholds,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkReport {
    pub status: Status,
    pub benchmark_name: String,
    pub profile: String,
    pub workload_signature: u64,
    pub topic_count: usize,
    pub min_sources_per_topic: usize,
    pub max_sources_per_topic: usize,
    pub source_kind_coverage_count: usize,
    pub source_count: usize,
    pub query_count: usize,
    pub accepted_sources: usize,
    pub failed_sources: usize,
    pub retrieval_successes: usize,
    pub injection_successes: usize,
    pub hit_at_k: f32,
    pub mean_reciprocal_rank: f32,
    pub total_source_tokens: u32,
    pub selected_context_tokens: u32,
    pub token_reduction_percent: f32,
    pub recall_at_k: f32,
    pub evidence_retention_at_k: f32,
    pub ingest_extract_elapsed_ms: u128,
    pub retrieve_total_elapsed_ms: u128,
    pub inject_total_elapsed_ms: u128,
    pub ingest_sources_per_sec: f64,
    pub retrieve_queries_per_sec: f64,
    pub inject_queries_per_sec: f64,
    pub retrieve_p50_ms: u128,
    pub retrieve_p95_ms: u128,
    pub inject_p50_ms: u128,
    pub inject_p95_ms: u128,
    pub avg_retrieved_blocks_per_query: f64,
    pub avg_selected_blocks_per_query: f64,
    pub avg_selected_tokens_per_query: f64,
    pub max_selected_tokens_per_query: u32,
    pub zero_hit_queries: usize,
    pub thresholds: ContextPipelineBenchmarkThresholds,
    pub threshold_passed: bool,
    pub threshold_violations: Vec<String>,
    pub per_query: Vec<ContextPipelineBenchmarkQueryReport>,
    pub source_kind_counts: BTreeMap<String, usize>,
    pub provider_counts: BTreeMap<String, usize>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkThresholds {
    pub min_hit_at_k: f32,
    pub min_mean_reciprocal_rank: f32,
    pub min_recall_at_k: f32,
    #[serde(default = "default_min_evidence_retention_at_k")]
    pub min_evidence_retention_at_k: f32,
    pub min_token_reduction_percent: f32,
    #[serde(default = "default_max_benchmark_selected_tokens_per_query")]
    pub max_selected_tokens_per_query: u32,
    pub max_retrieve_p50_ms: u128,
    pub max_retrieve_p95_ms: u128,
    pub min_ingest_sources_per_sec: f64,
    pub min_retrieve_queries_per_sec: f64,
    pub min_inject_queries_per_sec: f64,
}

impl Default for ContextPipelineBenchmarkThresholds {
    fn default() -> Self {
        default_benchmark_thresholds()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkQueryReport {
    pub query_id: String,
    pub expected_topic: String,
    pub expected_topic_source_count: usize,
    pub retrieved_blocks: usize,
    pub selected_blocks: usize,
    pub selected_tokens: u32,
    pub evidence_retained: bool,
    pub hit_rank: Option<usize>,
    pub reciprocal_rank: f32,
    pub retrieve_elapsed_ms: u128,
    pub inject_elapsed_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkSweepProfile {
    pub profile: String,
    pub source_count: usize,
    pub query_count: usize,
    pub max_events: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkSweepRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    #[serde(default = "default_benchmark_sweep_profiles")]
    pub profiles: Vec<ContextPipelineBenchmarkSweepProfile>,
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
    #[serde(default = "default_benchmark_thresholds")]
    pub thresholds: ContextPipelineBenchmarkThresholds,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkSweepReport {
    pub status: Status,
    pub benchmark_name: String,
    pub profile_count: usize,
    pub reports: Vec<ContextPipelineBenchmarkReport>,
    pub all_profiles_ready: bool,
    pub min_hit_at_k: f32,
    pub min_mean_reciprocal_rank: f32,
    pub min_evidence_retention_at_k: f32,
    pub min_token_reduction_percent: f32,
    pub max_retrieve_p95_ms: u128,
    pub max_inject_p95_ms: u128,
    pub total_sources: usize,
    pub total_queries: usize,
    pub profile_signatures: Vec<u64>,
    pub min_sources_per_topic: usize,
    pub max_sources_per_topic: usize,
    pub min_source_kind_coverage_count: usize,
    pub total_zero_hit_queries: usize,
    pub avg_selected_tokens_per_query: f64,
    pub max_selected_tokens_per_query: u32,
    pub all_thresholds_passed: bool,
    pub threshold_violations: Vec<String>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextRetrieveRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    #[serde(default)]
    pub node_hashes: Vec<u64>,
    #[serde(default)]
    pub query: String,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    #[serde(default = "default_retrieve_limit")]
    pub max_events: usize,
    #[serde(default)]
    pub min_confidence: f32,
    #[serde(default)]
    pub min_importance: f32,
    #[serde(default = "default_tiers")]
    pub tiers: Vec<ContextTier>,
    #[serde(default = "default_summary_fanout_node_limit")]
    pub max_summary_nodes: usize,
    #[serde(default = "default_event_fanout_node_limit")]
    pub max_event_nodes: usize,
    #[serde(default)]
    pub prefer_current_agent: bool,
    #[serde(default = "default_current_agent_scope_key")]
    pub current_agent_scope_key: String,
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextRetrieveReport {
    pub status: Status,
    pub blocks: Vec<ContextBlock>,
    pub node_count: usize,
    pub event_count: usize,
    #[serde(default)]
    pub query_understanding_debug: ContextQueryUnderstandingDebug,
    #[serde(default)]
    pub fanout_plan: ContextFanoutPlanReport,
    #[serde(default)]
    pub parity: ContextPipelineParityEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextFanoutPlanReport {
    pub strategy: String,
    pub namespace_node_candidates: usize,
    pub summary_candidate_nodes: usize,
    pub summary_selected_nodes: usize,
    pub event_expanded_nodes: usize,
    pub skipped_node_count: usize,
    pub summary_lookup_batches: usize,
    // Candidate nodes whose node record carries no vector at all -- un-embedded, scored by the
    // hybrid lexical pass instead. (Named for the retired separate-row fallback it once
    // counted; the rows are gone, so any nonzero here means the backfill has work to do.)
    #[serde(default)]
    pub l0_row_fallback_nodes: usize,
    /// Candidates whose L1 summary vector the node fetch could NOT supply, and which therefore
    /// still cost a per-node summary lookup. Zero is the steady state once summaries have been
    /// written by a build that copies the vector onto the node; anything else names how much of
    /// the second pass is still being paid for, which nothing else here would show.
    #[serde(default)]
    pub summary_lookup_nodes: usize,
    // Candidate vectors declined because their width did not match the query's, i.e. they were
    // written in a different embedding space. Counted separately from the un-embedded ones above
    // because the two ask for different repairs: an un-embedded node needs the backfill to run,
    // whereas a width conflict means the store holds vectors from two encoders and re-embedding
    // is the only fix. Nonzero here is the signal that would otherwise not exist -- comparing
    // across embedding spaces raises no error on its own.
    #[serde(default)]
    pub embedding_width_conflict_nodes: usize,
    /// Nodes declined because their vector was written by a DIFFERENT ENCODER at the same
    /// width. Separate from the width count on purpose: two widths means a provider outage
    /// seeded fallback vectors, while two encoders at one width means a model swap. Same
    /// symptom, different cause, different fix.
    pub embedding_model_conflict_nodes: usize,
    #[serde(default)]
    pub event_query_budget: usize,
    #[serde(default)]
    pub event_query_node_count: usize,
    #[serde(default)]
    pub event_query_returned_count: usize,
    pub secondary_index_filter_group_count: usize,
    pub selected_node_hashes: Vec<u64>,
    pub skipped_node_hashes: Vec<u64>,
    pub locality_keys: Vec<String>,
    pub current_agent_default_boost: u32,
    pub peer_agent_default_boost: u32,
    pub prefer_current_agent_configured: bool,
    pub peer_agent_demoted_by_default: bool,
    pub fallback_to_flat: bool,
    pub fanout_reduced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextQueryUnderstandingDebug {
    #[serde(default)]
    pub debug_schema: String,
    #[serde(default)]
    pub query_hash: u64,
    #[serde(default)]
    pub normalized_query_terms: Vec<String>,
    pub question_type: String,
    pub secondary_index_filter_groups: Vec<Vec<String>>,
    #[serde(default)]
    pub verbose_filter_groups: Vec<ContextQueryFilterGroupDebug>,
    #[serde(default)]
    pub filter_group_summary: ContextQueryFilterGroupSummaryDebug,
    pub candidates_passing_prefilter: usize,
    pub candidates_dropped_before_scoring: usize,
    pub tree_traversal_summary: ContextTreeTraversalDebug,
    pub prefilter_candidate_sample: Vec<ContextPrefilterCandidateDebug>,
    #[serde(default)]
    pub selected_refs: Vec<ContextSelectedRefDebug>,
    #[serde(default)]
    pub injection_ordering: Vec<ContextInjectionOrderingDebug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextQueryFilterGroupSummaryDebug {
    pub total_groups: usize,
    pub secondary_index_group_count: usize,
    pub lexical_group_count: usize,
    pub total_candidate_count: usize,
    pub total_matched_count: usize,
    pub total_dropped_count: usize,
    pub total_selected_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextQueryFilterGroupDebug {
    pub group_id: String,
    pub group_kind: String,
    pub terms: Vec<String>,
    pub candidate_ref_hashes: Vec<u64>,
    pub matched_ref_hashes: Vec<u64>,
    pub dropped_ref_hashes: Vec<u64>,
    pub selected_ref_hashes: Vec<u64>,
    pub candidate_count: usize,
    pub matched_count: usize,
    pub dropped_count: usize,
    pub selected_count: usize,
    #[serde(default)]
    pub candidate_decisions: Vec<ContextFilterGroupCandidateDecisionDebug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextFilterGroupCandidateDecisionDebug {
    pub ref_hash: u64,
    pub record_type: String,
    pub event_time_ms: u64,
    pub decision: String,
    pub reason: String,
    pub matched_terms: Vec<String>,
    pub candidate_terms: Vec<String>,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextTreeTraversalDebug {
    pub enabled: bool,
    pub fallback_reason: String,
    pub fallback_to_flat: bool,
    pub max_children_scored_per_parent: usize,
    pub selected_leaf_count: usize,
    pub selected_node_count: usize,
    pub selected_path_count: usize,
    pub summary_embeddings: Vec<String>,
    #[serde(default)]
    pub summary_embedding_candidate_count: usize,
    #[serde(default)]
    pub summary_embedding_selected_count: usize,
    #[serde(default)]
    pub summary_embedding_lookup_batches: usize,
    #[serde(default)]
    pub query_embedding_dimension: usize,
    #[serde(default)]
    pub query_embedding_provider: String,
    #[serde(default)]
    pub namespace_node_candidates: usize,
    #[serde(default)]
    pub event_expanded_node_count: usize,
    #[serde(default)]
    pub skipped_node_count: usize,
    pub top_k_per_layer: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPrefilterCandidateDebug {
    pub record_type: String,
    pub ref_hash: u64,
    pub node_hash: u64,
    pub event_time_ms: u64,
    pub node_path: Vec<String>,
    pub candidate_terms: Vec<String>,
    pub passes_secondary_index_prefilter: bool,
    #[serde(default)]
    pub drop_reason: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSelectedRefDebug {
    pub rank: usize,
    pub uri: String,
    pub source_ref: String,
    pub tier: ContextTier,
    pub ref_hash: u64,
    pub node_hash: u64,
    pub event_time_ms: u64,
    pub relevance_score: u32,
    pub matched_filter_groups: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextInjectionOrderingDebug {
    pub prompt_rank: usize,
    pub source_ref: String,
    pub tier: ContextTier,
    pub ref_hash: u64,
    pub token_estimate: u32,
    pub selection_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBlock {
    pub uri: String,
    pub tier: ContextTier,
    pub node_hash: u64,
    pub event_time_ms: u64,
    pub text: String,
    pub estimated_tokens: u32,
    pub source_ref: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextInjectRequest {
    pub retrieve: ContextRetrieveRequest,
    pub prompt: String,
    pub session_hash: u64,
    pub query_id: String,
    #[serde(default = "default_max_prompt_tokens")]
    pub max_prompt_tokens: u32,
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextInjectReport {
    pub status: Status,
    pub provider: ContextModelProviderConfig,
    pub injected_prompt: String,
    pub selected_blocks: Vec<ContextBlock>,
    pub blocked_blocks: Vec<ContextBlock>,
    pub audit: ContextPackAudit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextWorkflowStateReport {
    pub status: Status,
    pub providers: Vec<ContextModelProviderConfig>,
    pub context_model_descriptors: Vec<ContextModelDescriptor>,
    pub reference_model_profiles: Vec<ContextReferenceModelProfile>,
    pub reference_parity_cases: Vec<ContextReferenceParityCase>,
    pub reference_parity_categories: Vec<String>,
    pub open_model_provider_packaged: bool,
    pub open_model_local_run_proven: bool,
    pub vlm_provider_configured: bool,
    pub vlm_benchmark_proven: bool,
    pub policy: ContextWorkflowPolicy,
    pub parity: ContextPipelineParityEvidence,
    pub reference_comparison: String,
    pub supported_routes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextReferenceModelProfile {
    pub profile_name: String,
    pub provider_name: String,
    pub provider_kind: ContextProviderKind,
    pub base_url: String,
    pub chat_model: String,
    pub vlm_model: String,
    pub embedding_model: String,
    pub capabilities: Vec<String>,
    pub notes: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextReferenceParityCase {
    pub case_name: String,
    pub category: String,
    pub query: String,
    pub positive_memory: String,
    pub stale_memory: String,
    pub expected_terms: Vec<String>,
    pub expected_model_profile: String,
    pub uses_vlm: bool,
    pub benchmark_proven: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPipelineManageReport {
    pub status: Status,
    pub pipeline_ready: bool,
    pub management_ready: bool,
    pub ingestion_extraction_ready: bool,
    pub retrieval_ready: bool,
    pub injection_ready: bool,
    pub provider_count: usize,
    pub supported_routes: Vec<String>,
    pub stages: Vec<String>,
    pub stage_reports: Vec<ContextPipelineStageReport>,
    pub provider_names: Vec<String>,
    pub policy_controls: Vec<String>,
    pub parity: ContextPipelineParityEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPipelineStageReport {
    pub stage: String,
    pub ready: bool,
    pub route: Option<String>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPipelineParityEvidence {
    pub native_context_models_ready: bool,
    pub native_context_model_ids_ready: bool,
    pub native_context_timeline_semantics_ready: bool,
    pub native_context_validation_limits_ready: bool,
    pub reference_tiers_ready: bool,
    pub extraction_stage_ready: bool,
    pub retrieval_stage_ready: bool,
    pub injection_stage_ready: bool,
    pub index_refs_ready: bool,
    pub pack_audit_ready: bool,
    pub summary_dirty_ready: bool,
    pub restart_replay_ready: bool,
    pub shared_store_sync_ready: bool,
    pub shared_store_async_ready: bool,
    pub raft_read_ready: bool,
    pub unified_corpus_ready: bool,
    pub pipeline_ready: bool,
    pub evidence: Vec<String>,
}

impl Default for ContextPipelineParityEvidence {
    fn default() -> Self {
        context_pipeline_parity_evidence()
    }
}

pub fn context_pipeline_parity_evidence() -> ContextPipelineParityEvidence {
    let evidence = vec![
        "ContextNode/Event/IndexRef/PackAudit/SummaryDirty model aliases and protobuf wire encoders are implemented"
            .to_string(),
        "Context model ids 9-13 are exposed as first-class Rust descriptors".to_string(),
        "Context timeline fanout, key shapes, range windows, and validation limits are enforced"
            .to_string(),
        "Hierarchical L0/L1/L2 tiers are produced during extraction and consumed during retrieval/injection"
            .to_string(),
        "Context extraction persists node, event, index-ref, and dirty-summary commands through TemporalEngine"
            .to_string(),
        "Context injection persists ContextPackAudit selected and blocked refs".to_string(),
        "Context workflow harness validates local restart, shared-store sync/async replay, Raft replica reads, and unified conformance context corpus evidence"
            .to_string(),
    ];
    ContextPipelineParityEvidence {
        native_context_models_ready: true,
        native_context_model_ids_ready: true,
        native_context_timeline_semantics_ready: true,
        native_context_validation_limits_ready: true,
        reference_tiers_ready: true,
        extraction_stage_ready: true,
        retrieval_stage_ready: true,
        injection_stage_ready: true,
        index_refs_ready: true,
        pack_audit_ready: true,
        summary_dirty_ready: true,
        restart_replay_ready: true,
        shared_store_sync_ready: true,
        shared_store_async_ready: true,
        raft_read_ready: true,
        unified_corpus_ready: true,
        pipeline_ready: true,
        evidence,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextWorkflowPolicy {
    #[serde(default = "default_allowed_context_provider_kinds")]
    pub allowed_provider_kinds: Vec<ContextProviderKind>,
    #[serde(default)]
    pub allowed_models: Vec<String>,
    #[serde(default = "default_max_extract_body_bytes")]
    pub max_extract_body_bytes: usize,
    #[serde(default = "default_max_prompt_tokens")]
    pub max_prompt_tokens: u32,
    #[serde(default = "default_true")]
    pub pii_filtering_enabled: bool,
    #[serde(default = "default_true")]
    pub tenant_isolation_required: bool,
    #[serde(default = "default_context_rate_limit_per_minute")]
    pub rate_limit_per_minute: u32,
    #[serde(default = "default_context_provider_failure_budget")]
    pub provider_failure_budget: u32,
}

impl Default for ContextWorkflowPolicy {
    fn default() -> Self {
        Self {
            allowed_provider_kinds: default_allowed_context_provider_kinds(),
            allowed_models: Vec::new(),
            max_extract_body_bytes: default_max_extract_body_bytes(),
            max_prompt_tokens: default_max_prompt_tokens(),
            pii_filtering_enabled: true,
            tenant_isolation_required: true,
            rate_limit_per_minute: default_context_rate_limit_per_minute(),
            provider_failure_budget: default_context_provider_failure_budget(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextWorkflowPolicyReport {
    pub status: Status,
    pub provider_allowed: bool,
    pub model_allowed: bool,
    pub body_size_allowed: bool,
    pub prompt_size_allowed: bool,
    pub pii_filtering_applied: bool,
    pub tenant_isolation_applied: bool,
    pub rate_limit_allowed: bool,
    pub provider_failure_budget_allowed: bool,
    pub sanitized_text: String,
}

/// Single-source extraction (codex-hook path). Gates L1 by source richness so
/// thin events do not pay for the richer L1 tier.
pub fn extract_context(
    engine: &TemporalEngine,
    request: ContextExtractRequest,
) -> ContextExtractReport {
    extract_context_gated(engine, request, true)
}

/// Extraction with explicit control over L1 gating. The batch ingest path
/// (`ingest_extract_context`) passes `gate_l1 = false` so bulk resource/skill
/// docs always produce L1 and keep their fixed fanout contract (2 summaries +
/// 2 embeddings per extract -- the node and its level-2 summary are built from the
/// same `l1` text and share the one vector it produces).
pub(crate) fn extract_context_gated(
    engine: &TemporalEngine,
    request: ContextExtractRequest,
    gate_l1: bool,
) -> ContextExtractReport {
    let provider = normalize_provider(request.provider.clone());
    let summaries = match context_summaries_for_extract(&provider, &request) {
        Ok(summaries) => summaries,
        Err(status) => {
            if let Some(fallback) = provider.fallback_provider.as_deref() {
                let fallback = normalize_provider(fallback.clone());
                match context_summaries_for_extract(&fallback, &request) {
                    Ok(mut summaries) => {
                        summaries.provider = fallback;
                        summaries.provider.provider_name = format!(
                            "{}+fallback:{}",
                            provider.provider_name, summaries.provider.provider_name
                        );
                        summaries
                    }
                    Err(fallback_status) => {
                        return empty_extract_report(
                            fallback_status,
                            provider,
                            request.tenant_hash,
                            request.timestamp_ms,
                        );
                    }
                }
            } else {
                return empty_extract_report(
                    status,
                    provider,
                    request.tenant_hash,
                    request.timestamp_ms,
                );
            }
        }
    };
    let provider = summaries.provider;
    if !summaries.status.ok {
        return ContextExtractReport {
            status: summaries.status,
            provider,
            embedding_generation: ContextEmbeddingGenerationReport::default(),
            node: empty_node(),
            event: empty_event(),
            index_ref: ContextIndexRef {
                primary_node_hash: 0,
                primary_event_time_ms: 0,
                event_id_hash: 0,
            },
            dirty_marker: ContextDirtyNode {
                node_hash: 0,
                first_event_time_ms: request.timestamp_ms,
                last_event_time_ms: request.timestamp_ms,
                reason: 0,
