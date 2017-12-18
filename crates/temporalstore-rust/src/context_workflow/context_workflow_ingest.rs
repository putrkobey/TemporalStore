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
