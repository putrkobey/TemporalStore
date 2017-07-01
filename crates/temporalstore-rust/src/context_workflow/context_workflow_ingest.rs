// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Context ingest + resource/skill secondary-index verification, split from context_workflow.rs.
use super::*;

/// Ingest with the public API's historical full fanout: every source emits its L1 summary.
pub fn ingest_extract_context(
    engine: &TemporalEngine,
    request: ContextIngestExtractRequest,
) -> ContextIngestExtractReport {
    ingest_extract_context_with_l1_gate(engine, request, false)
}

/// The same, with the frugal L1 gate live hook ingestion uses: only a source whose body warrants
/// one gets an L1 summary.
///
/// `MATRIXARK_CONTEXT_INGEST_GATE_L1` used to carry this in, because `ingest_extract_context` had
/// nowhere to put it -- so `context_batch_ingest` set it for its whole process and the gate was on
/// for that binary and off for every other caller. `extract_context_gated` has taken the value as
/// an argument all along; this just gives the caller a way to supply one.
pub fn ingest_extract_context_with_l1_gate(
    engine: &TemporalEngine,
    request: ContextIngestExtractRequest,
    gate_l1: bool,
) -> ContextIngestExtractReport {
    // Hold the per-record index reconstruct for this whole ingest and do it once below. Each
    // Context* write would otherwise rebuild the first-index over every live page in the shard --
    // eight walks of the store for one add.
    let reconstruct_window = crate::engine::IngestReconstructWindow::open();
    let policy = ContextWorkflowPolicy::default();
    let mut extracts = Vec::new();
    let mut failed_sources = Vec::new();
    let mut node_hashes = Vec::new();
    let mut source_kind_counts = BTreeMap::new();
    let mut provider_counts = BTreeMap::new();
    let provider = normalize_provider(request.provider.clone());
    let source_count = request.sources.len();
    let start_time_ms = request.start_time_ms;
    let end_time_ms = request.end_time_ms;

    for mut source in request.sources {
        source.shard_id = request.shard_id;
        source.tenant_hash = request.tenant_hash;
        if source.provider.provider_name.is_empty() {
            source.provider = provider.clone();
        }
        *source_kind_counts
            .entry(context_source_kind_name(source.source_kind).to_string())
            .or_insert(0) += 1;
        *provider_counts
            .entry(source.provider.provider_name.clone())
            .or_insert(0) += 1;
        let policy_report = validate_context_extract_policy(&policy, &source);
        if !policy_report.status.ok {
            failed_sources.push(ContextIngestSourceFailure {
                source_id: source.source_id,
                status: policy_report.status,
            });
            continue;
        }
        let source_id = source.source_id.clone();
        // Keep the public API's historical full-fanout default, while allowing
        // local-context backfill to opt into the same frugal L1 gate as live
        // hook ingestion.
        let extract = extract_context_gated(engine, source, gate_l1);
        if extract.status.ok {
            node_hashes.push(extract.node.node_hash);
            extracts.push(extract);
        } else {
            failed_sources.push(ContextIngestSourceFailure {
                source_id,
                status: extract.status.clone(),
            });
        }
    }

    // No reconstruct: the writes above maintain the index themselves now, so the window has
    // nothing to catch up on. It stays open across the ingest only to keep the per-record
    // fallback rebuild from firing for a write maintenance does not cover -- and if one is not
    // covered, the fallback runs at that write, not here.
    drop(reconstruct_window);

    node_hashes.sort_unstable();
    node_hashes.dedup();
    let failed = failed_sources.len();
    let accepted = extracts.len();
    let status = if failed == 0 {
        Status::ok()
    } else if accepted > 0 {
        Status::error(
            "partial_context_ingest_extract_failure",
            format!("{failed} context sources failed"),
        )
    } else {
        Status::error(
            "context_ingest_extract_failed",
            "all context sources failed ingestion/extraction",
        )
    };
    let retrieve_request = ContextRetrieveRequest {
        shard_id: request.shard_id,
        tenant_hash: request.tenant_hash,
        node_hashes: node_hashes.clone(),
        query: request.query,
        start_time_ms: request.start_time_ms,
        end_time_ms: request.end_time_ms,
        max_events: request.max_events,
        min_confidence: 0.0,
        min_importance: 0.0,
        tiers: default_tiers(),
        max_summary_nodes: default_summary_fanout_node_limit(),
        max_event_nodes: default_event_fanout_node_limit(),
        prefer_current_agent: false,
        current_agent_scope_key: default_current_agent_scope_key(),
        provider: request.provider,
    };
    let summary = ContextIngestExtractSummary {
        source_count,
        accepted,
        failed,
        unique_node_count: node_hashes.len(),
        extracted_node_count: extracts.len(),
        extracted_event_count: extracts.len(),
        extracted_index_ref_count: extracts.len(),
        extracted_dirty_marker_count: extracts.len(),
        extracted_summary_ref_count: extracts
            .iter()
            .map(|extract| extract.summary_refs.len())
            .sum(),
        retrieval_node_count: retrieve_request.node_hashes.len(),
        source_kind_counts,
        provider_counts,
        start_time_ms,
        end_time_ms,
    };

    ContextIngestExtractReport {
        status,
        accepted,
        failed,
        summary,
        extracts,
        failed_sources,
        node_hashes,
        retrieve_request,
        parity: context_pipeline_parity_evidence(),
    }
}

pub fn ingest_resource_skill_context(
    engine: &TemporalEngine,
    request: ContextResourceSkillIngestRequest,
) -> ContextResourceSkillIngestReport {
    let provider = normalize_provider(request.provider.clone());
    let mut resources = Vec::new();
    let mut skills = Vec::new();
    let mut sources = Vec::new();
    let mut resource_ref_by_source = BTreeMap::new();
    let mut skill_ref_by_source = BTreeMap::new();
    let mut timestamp_ms = request.start_time_ms.max(1);

    for resource_request in request.resources {
        // An oversized payload gets stored WHOLE in the engine-owned blob store before
        // parsing, so the synthesized external URI on the report is real and fetchable --
        // chunks stay searchable in records while the original attachment lives beside the
        // engine, one TemporalStore holding everything.
        let oversized_payload = if resource_request.text.as_bytes().len()
            > default_context_resource_max_inline_bytes()
        {
            Some(resource_request.text.clone())
        } else {
            None
        };
        let mut resource = parse_context_resource(resource_request);
        if let Some(payload) = oversized_payload {
            match engine.resource_blob_put(request.tenant_hash, payload.as_bytes()) {
                Ok((uri, _size, _hash)) => resource.external_object_uri = uri,
                Err(err) => resource
                    .parser_warnings
                    .push(format!("resource blob store put failed: {err}")),
            }
        }
        for chunk in &resource.chunks {
            resource_ref_by_source.insert(chunk.source_ref.clone(), resource.raw_uri.clone());
            sources.push(ContextExtractRequest {
                shard_id: request.shard_id,
                tenant_hash: request.tenant_hash,
                source_kind: ContextSourceKind::Document,
                source_id: chunk.source_ref.clone(),
                title: chunk
                    .metadata
                    .get("heading")
                    .cloned()
                    .unwrap_or_else(|| resource.resource_title.clone()),
                body: chunk.text.clone(),
                timestamp_ms,
                provider: provider.clone(),
            });
            timestamp_ms = timestamp_ms.saturating_add(1);
        }
        resources.push(resource);
    }

    for skill_input in request.skills {
        let skill = parse_context_skill_markdown(skill_input.raw_uri, skill_input.text);
        if skill.enabled {
            for chunk in &skill.resource.chunks {
                skill_ref_by_source.insert(chunk.source_ref.clone(), skill.skill_name.clone());
                sources.push(ContextExtractRequest {
                    shard_id: request.shard_id,
                    tenant_hash: request.tenant_hash,
                    source_kind: ContextSourceKind::Document,
                    source_id: chunk.source_ref.clone(),
                    title: format!("skill:{}", skill.skill_name),
                    body: chunk.text.clone(),
                    timestamp_ms,
                    provider: provider.clone(),
                });
                timestamp_ms = timestamp_ms.saturating_add(1);
            }
        }
        skills.push(skill);
    }
    let skill_registry = context_skill_registry_from_parsed(&skills, request.start_time_ms);
    let skill_selection = if skill_registry.entries.is_empty() {
        ContextSkillSelectionReport::default()
    } else {
        select_context_skills_for_retrieval(ContextSkillSelectionRequest {
            query: request.query.clone(),
            owner_scope: String::new(),
            tool_name: "context_workflow_harness".to_string(),
            include_disabled: false,
            allowed_scope_layers: Vec::new(),
            limit: default_skill_selection_limit(),
            registry: skill_registry.entries.clone(),
        })
    };

    let ingest = ingest_extract_context(
        engine,
        ContextIngestExtractRequest {
            shard_id: request.shard_id,
            tenant_hash: request.tenant_hash,
            sources,
            query: request.query.clone(),
            start_time_ms: request.start_time_ms,
            end_time_ms: request.end_time_ms,
            max_events: request.max_events,
            provider,
        },
    );
    let mut fanout = ContextResourceSkillModelFanoutReport {
        node_count: ingest.extracts.len(),
        event_count: ingest.extracts.len(),
        slab_count: ingest.extracts.len(),
        embedding_count: ingest.extracts.len().saturating_mul(3),
        summary_count: ingest.extracts.len().saturating_mul(2),
        dirty_marker_count: ingest.extracts.len(),
        ..ContextResourceSkillModelFanoutReport::default()
    };
    let mut secondary_indexes = ContextResourceSkillSecondaryIndexReport::default();

    for extract in &ingest.extracts {
        let entity_ref = format!("entity:{}", extract.node.canonical_name);
        let entity_hash = stable_hash64(&entity_ref);
        let child_ref = ContextChildRef {
            parent_hash: stable_hash64(&format!("resource-skill-root:{}", request.tenant_hash)),
            child_hash: extract.node.node_hash,
            updated_at_ms: extract.event.event_time_ms,
        };
        let entity = ContextEntity {
            entity_hash,
            node_hash: extract.node.node_hash,
            entity_type: source_kind_code(ContextSourceKind::Document),
            name: extract.node.canonical_name.clone(),
            value: extract.source_ref.clone(),
            updated_at_ms: extract.event.event_time_ms,
            valid_from_ms: extract.event.event_time_ms,
            confidence: 1.0,
            source_event_hashes: vec![extract.event.event_id_hash],
            vector: Vec::new(),
        };
        let compression = ContextCompressionEvent {
            compression_id_hash: stable_hash64(&format!(
                "ctx-resource-skill-compress:{}:{}",
                extract.source_ref, extract.event.event_time_ms
            )),
            node_hash: extract.node.node_hash,
            source_start_ms: extract.event.event_time_ms,
            source_end_ms: extract.event.event_time_ms.saturating_add(1),
            compressed_time_ms: extract.event.event_time_ms.saturating_add(1),
            summary: extract.compact_summary_ref.clone(),
        };
