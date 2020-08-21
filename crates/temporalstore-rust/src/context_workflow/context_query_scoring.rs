// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::types::{ContextEvent, Status};

use super::{
    context_embeddings_for_extract, normalize_provider, truncate_words, ContextBlock,
    ContextFilterGroupCandidateDecisionDebug, ContextInjectionOrderingDebug,
    ContextModelProviderConfig, ContextPrefilterCandidateDebug, ContextQueryFilterGroupDebug,
    ContextQueryFilterGroupSummaryDebug, ContextQueryUnderstandingDebug, ContextRetrieveRequest,
    ContextSelectedRefDebug, ContextTier, ContextTreeTraversalDebug,
};
pub(super) fn tier_rank(tier: ContextTier) -> u8 {
    match tier {
        ContextTier::L0 => 0,
        ContextTier::L1 => 1,
        ContextTier::L2 => 2,
    }
}

#[derive(Debug, Clone)]
pub(super) struct ContextQueryPlan {
    pub terms: Vec<String>,
    pub term_groups: Vec<Vec<String>>,
    pub adjacent_phrases: Vec<String>,
    pub topic_phrase: Option<String>,
    pub requests_latest: bool,
    pub requests_temporal_reasoning: bool,
    pub requests_after: bool,
    pub requests_before: bool,
    pub requests_correction: bool,
    pub requests_reminder: bool,
    pub requests_contrastive_update: bool,
    pub requests_social_link: bool,
    pub requests_schedule_detail: bool,
    pub requests_quantity_detail: bool,
    pub requests_alias_detail: bool,
}

pub(super) fn context_query_plan(query: &str) -> ContextQueryPlan {
    let terms = context_query_terms(query);
    let term_groups = context_query_term_groups_from_terms(&terms);
    ContextQueryPlan {
        adjacent_phrases: context_query_adjacent_phrases(&terms),
        topic_phrase: context_query_topic_phrase(query),
        requests_latest: context_query_requests_latest(&terms),
        requests_temporal_reasoning: context_query_requests_temporal_reasoning(&terms),
        requests_after: context_query_requests_after(&terms),
        requests_before: context_query_requests_before(&terms),
        requests_correction: context_query_requests_correction(&terms),
        requests_reminder: context_query_requests_reminder(&terms),
        requests_contrastive_update: context_query_requests_contrastive_update(&terms),
        requests_social_link: context_query_requests_social_link(&terms),
        requests_schedule_detail: context_query_requests_schedule_detail(&terms),
        requests_quantity_detail: context_query_requests_quantity_detail(&terms),
        requests_alias_detail: context_query_requests_alias_detail(&terms),
        terms,
        term_groups,
    }
}

#[allow(dead_code)]
pub(super) fn context_query_matches(query: &str, text: &str) -> bool {
    let plan = context_query_plan(query);
    context_query_matches_plan(&plan, text)
}

pub(super) fn context_query_matches_plan(plan: &ContextQueryPlan, text: &str) -> bool {
    let text_lower = text.to_ascii_lowercase();
    let text_normalized = context_normalize_for_match(text);
    if let Some(topic_phrase) = plan.topic_phrase.as_ref() {
        if context_text_matches_term(&text_lower, &text_normalized, topic_phrase.as_str()) {
            return true;
        }
    }
    if plan.term_groups.is_empty() {
        return true;
    }
    let matched_groups = plan
        .term_groups
        .iter()
        .filter(|group| {
            group
                .iter()
                .any(|term| context_text_matches_term(&text_lower, &text_normalized, term))
        })
        .count();
    matched_groups > 0
}

pub(super) fn context_query_understanding_debug_for_plan(
    request: &ContextRetrieveRequest,
    plan: &ContextQueryPlan,
) -> ContextQueryUnderstandingDebug {
    let filter_groups = context_query_secondary_index_filter_groups(&plan.terms);
    ContextQueryUnderstandingDebug {
        debug_schema: "matrixark_context_query_debug_v1".to_string(),
        query_hash: stable_hash64(&request.query),
        normalized_query_terms: plan.terms.clone(),
        question_type: context_query_question_type(&plan.terms),
        secondary_index_filter_groups: filter_groups.clone(),
        verbose_filter_groups: context_query_verbose_filter_groups(&filter_groups),
        filter_group_summary: ContextQueryFilterGroupSummaryDebug {
            total_groups: filter_groups.len(),
            secondary_index_group_count: filter_groups
                .iter()
                .filter(|group| group.iter().any(|term| !term.starts_with("query_term:")))
                .count(),
            lexical_group_count: filter_groups
                .iter()
                .filter(|group| group.iter().all(|term| term.starts_with("query_term:")))
                .count(),
            ..ContextQueryFilterGroupSummaryDebug::default()
        },
        candidates_passing_prefilter: 0,
        candidates_dropped_before_scoring: 0,
        tree_traversal_summary: ContextTreeTraversalDebug {
            enabled: true,
            fallback_reason: String::new(),
            fallback_to_flat: false,
            max_children_scored_per_parent: request.max_events.max(1),
            selected_leaf_count: 0,
            selected_node_count: 0,
            selected_path_count: 0,
            summary_embedding_candidate_count: 0,
            summary_embedding_selected_count: 0,
            summary_embedding_lookup_batches: 0,
            query_embedding_dimension: 0,
            query_embedding_provider: String::new(),
            namespace_node_candidates: 0,
            event_expanded_node_count: 0,
            skipped_node_count: 0,
            summary_embeddings: Vec::new(),
            top_k_per_layer: request.max_events.max(1),
            ..ContextTreeTraversalDebug::default()
        },
        prefilter_candidate_sample: Vec::new(),
        selected_refs: Vec::new(),
        injection_ordering: Vec::new(),
    }
}

pub(super) fn context_query_debug_record_candidate(
    debug: &mut ContextQueryUnderstandingDebug,
    tenant_hash: u64,
    node_hash: u64,
    event: &ContextEvent,
    passes_prefilter: bool,
) {
    let ref_hash = stable_hash64(&format!(
        "ctx-prefilter:{tenant_hash}:{node_hash}:{}:{}",
        event.event_time_ms, event.event_id_hash
    ));
    let candidate_terms = context_event_candidate_terms(event);
    let event_text = event.text.as_str();
    for group in &mut debug.verbose_filter_groups {
        group.candidate_count += 1;
        push_debug_hash(&mut group.candidate_ref_hashes, ref_hash);
        let matched_terms =
            context_debug_filter_group_matched_terms(group, &candidate_terms, event_text);
        let matched_group = passes_prefilter && !matched_terms.is_empty();
        if matched_group {
            group.matched_count += 1;
            push_debug_hash(&mut group.matched_ref_hashes, ref_hash);
        } else {
            group.dropped_count += 1;
            push_debug_hash(&mut group.dropped_ref_hashes, ref_hash);
        }
        push_filter_group_decision(
            group,
            ContextFilterGroupCandidateDecisionDebug {
                ref_hash,
                record_type: "context_event".to_string(),
                event_time_ms: event.event_time_ms,
                decision: if matched_group { "matched" } else { "dropped" }.to_string(),
                reason: if matched_group {
                    "matched_filter_group".to_string()
                } else if passes_prefilter {
                    "filter_group_term_miss".to_string()
                } else {
                    "secondary_index_prefilter_miss".to_string()
                },
                matched_terms,
                candidate_terms: candidate_terms.clone(),
                text: truncate_words(&event.text, 24),
            },
        );
    }
    if passes_prefilter {
        debug.candidates_passing_prefilter += 1;
    } else {
        debug.candidates_dropped_before_scoring += 1;
    }
    if debug.question_type == "match_all" {
        return;
    }
    if debug.prefilter_candidate_sample.len() >= 12 {
        return;
    }
    debug
        .prefilter_candidate_sample
        .push(ContextPrefilterCandidateDebug {
            record_type: "context_event".to_string(),
            ref_hash,
            node_hash,
            event_time_ms: event.event_time_ms,
            node_path: vec![format!("tenant:{tenant_hash}"), format!("node:{node_hash}")],
            candidate_terms,
            passes_secondary_index_prefilter: passes_prefilter,
            drop_reason: if passes_prefilter {
                String::new()
            } else {
                "secondary_index_prefilter_miss".to_string()
            },
            text: truncate_words(&event.text, 32),
        });
}

pub(super) fn context_query_debug_finalize(
    debug: &mut ContextQueryUnderstandingDebug,
    plan: &ContextQueryPlan,
    blocks: &[ContextBlock],
    node_count: usize,
    tiers: &[ContextTier],
) {
    debug.tree_traversal_summary.selected_leaf_count = blocks
        .iter()
        .filter(|block| block.tier == ContextTier::L2)
        .count();
    debug.tree_traversal_summary.selected_node_count = node_count;
    debug.tree_traversal_summary.selected_path_count = blocks.len();
    debug
        .tree_traversal_summary
        .summary_embeddings
        .extend(tiers.iter().filter_map(|tier| match tier {
            ContextTier::L0 => Some("node_l0".to_string()),
            ContextTier::L1 => Some("node_l1".to_string()),
            ContextTier::L2 => None,
        }));
    debug.tree_traversal_summary.summary_embeddings.sort();
    debug.tree_traversal_summary.summary_embeddings.dedup();
    debug.selected_refs = blocks
        .iter()
        .enumerate()
        .map(|(index, block)| {
            let ref_hash = context_selected_ref_hash(block);
            let matched_filter_groups = debug
                .verbose_filter_groups
                .iter_mut()
