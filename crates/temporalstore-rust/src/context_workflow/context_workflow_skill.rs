// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::resource::{
    default_resource_max_chunk_chars, default_resource_overlap_chars, default_resource_parser_name,
    extract_markdown_link_refs, parse_context_resource, slugify_context_resource, split_paragraphs,
};
use super::*;

pub fn parse_context_skill_markdown(
    raw_uri: impl Into<String>,
    text: impl Into<String>,
) -> ContextSkillParseReport {
    let raw_uri = raw_uri.into();
    let text = text.into();
    let front_matter = parse_skill_front_matter(&text);
    let skill_name = front_matter
        .get("name")
        .cloned()
        .unwrap_or_else(|| infer_skill_name_from_uri(&raw_uri));
    let description = front_matter
        .get("description")
        .cloned()
        .unwrap_or_else(|| first_markdown_paragraph(&text));
    let tag_refs =
        parse_skill_front_matter_list(&front_matter, &["tags", "tag", "categories", "category"]);
    let allowed_tools = parse_skill_front_matter_list(
        &front_matter,
        &["allowed_tools", "allowed_tool", "tools", "tooling"],
    );
    let triggers =
        parse_skill_front_matter_list(&front_matter, &["triggers", "trigger", "activation"]);
    let model_refs =
        parse_skill_front_matter_list(&front_matter, &["models", "model", "providers", "provider"]);
    let owner_scope = front_matter
        .get("owner_scope")
        .or_else(|| front_matter.get("owner"))
        .cloned()
        .unwrap_or_else(|| "user".to_string());
    let scope = context_scope_descriptor(&owner_scope);
    let enabled = parse_skill_enabled(&front_matter);
    let precedence = parse_skill_precedence(
        front_matter
            .get("precedence")
            .or_else(|| front_matter.get("priority"))
            .map(String::as_str),
    );
    let tool_refs = parse_skill_section_items(&text, &["tools", "tooling", "commands"], true);
    let instruction_refs = parse_skill_section_items(
        &text,
        &["instructions", "workflow", "steps", "when to use"],
        false,
    );
    let resource_refs = parse_skill_section_items(&text, &["resources", "references"], true);
    let example_refs = parse_skill_section_items(&text, &["examples"], false);
    let resource = parse_context_resource(ContextResourceParseRequest {
        raw_uri: raw_uri.clone(),
        resource_type: Some("skill".to_string()),
        text,
        max_chunk_chars: default_resource_max_chunk_chars(),
        overlap_chars: default_resource_overlap_chars(),
        chunk_hash_base: None,
        owner_scope: owner_scope.clone(),
        version: front_matter
            .get("version")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string()),
        watch_interval_minutes: 0,
        parser_name: default_resource_parser_name(),
    });
    let capability_refs = resource
        .chunks
        .iter()
        .filter_map(|chunk| chunk.metadata.get("heading_slug"))
        .filter(|slug| {
            matches!(
                slug.as_str(),
                "when-to-use"
                    | "tools"
                    | "instructions"
                    | "resources"
                    | "references"
                    | "examples"
                    | "capabilities"
            )
        })
        .cloned()
        .collect();
    ContextSkillParseReport {
        status: Status::ok(),
        skill_name,
        description,
        source_ref: raw_uri,
        version: front_matter
            .get("version")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string()),
        owner_scope,
        scope,
        enabled,
        precedence,
        front_matter,
        tag_refs,
        capability_refs,
        allowed_tools,
        triggers,
        model_refs,
        tool_refs,
        instruction_refs,
        resource_refs,
        example_refs,
        parser_warnings: resource.parser_warnings.clone(),
        resource,
    }
}

pub fn context_skill_registry_from_parsed(
    skills: &[ContextSkillParseReport],
    updated_at_ms: u64,
) -> ContextSkillRegistryReport {
    let entries = skills
        .iter()
        .map(|skill| ContextSkillRegistryEntry {
            skill_name: skill.skill_name.clone(),
            source_ref: skill.source_ref.clone(),
            description: skill.description.clone(),
            version: skill.version.clone(),
            owner_scope: skill.owner_scope.clone(),
            scope: skill.scope.clone(),
            enabled: skill.enabled,
            precedence: skill.precedence,
            triggers: normalized_unique_strings(skill.triggers.clone()),
            allowed_tools: normalized_unique_strings(skill.allowed_tools.clone()),
            tag_refs: normalized_unique_strings(skill.tag_refs.clone()),
            model_refs: normalized_unique_strings(skill.model_refs.clone()),
            updated_at_ms,
        })
        .collect::<Vec<_>>();
    context_skill_registry_report(entries, Vec::new())
}

pub fn update_context_skill_registry(
    mut entries: Vec<ContextSkillRegistryEntry>,
    updates: Vec<ContextSkillRegistryUpdate>,
) -> ContextSkillRegistryReport {
    let mut version_updates = Vec::new();
    for update in updates {
        if let Some(entry) = entries
            .iter_mut()
            .find(|entry| entry.skill_name == update.skill_name)
        {
            if let Some(enabled) = update.enabled {
                entry.enabled = enabled;
            }
            if let Some(precedence) = update.precedence {
                entry.precedence = precedence;
            }
            if let Some(owner_scope) = update.owner_scope {
                entry.owner_scope = owner_scope;
                entry.scope = context_scope_descriptor(&entry.owner_scope);
            }
            if let Some(triggers) = update.triggers {
                entry.triggers = normalized_unique_strings(triggers);
            }
            if let Some(allowed_tools) = update.allowed_tools {
                entry.allowed_tools = normalized_unique_strings(allowed_tools);
            }
            if let Some(version) = update.version {
                if entry.version != version {
                    version_updates.push(format!(
                        "{}:{}->{}",
                        entry.skill_name, entry.version, version
                    ));
                    entry.version = version;
                }
            }
            if update.updated_at_ms > 0 {
                entry.updated_at_ms = update.updated_at_ms;
            }
        }
    }
    context_skill_registry_report(entries, version_updates)
}

pub fn select_context_skills_for_retrieval(
    request: ContextSkillSelectionRequest,
) -> ContextSkillSelectionReport {
    let query_terms = context_query_terms(&request.query);
    let requested_scope = context_scope_descriptor(&request.owner_scope);
    let owner_scope_filter_enabled = !request.owner_scope.trim().is_empty();
    let tool_name = request.tool_name.trim();
    let mut selected = Vec::new();
    let mut skipped_disabled = Vec::new();
    let mut skipped_owner_scope = Vec::new();
    let mut skipped_tool = Vec::new();
    let mut scope_resolution_order = Vec::new();
    let mut agent_producers = Vec::new();
    let mut shared_graph_scopes = BTreeSet::new();

    for entry in request.registry {
        if !entry.enabled && !request.include_disabled {
            skipped_disabled.push(entry.skill_name);
            continue;
        }
        let entry_scope = context_skill_entry_scope(&entry);
        if !request.allowed_scope_layers.is_empty()
            && !request
                .allowed_scope_layers
                .iter()
                .any(|layer| *layer == entry_scope.layer)
        {
            skipped_owner_scope.push(entry.skill_name);
            continue;
        }
        if owner_scope_filter_enabled && !context_scope_matches(&requested_scope, &entry_scope) {
            skipped_owner_scope.push(entry.skill_name);
            continue;
        }
        let allowed_tool_match = tool_name.is_empty()
            || entry.allowed_tools.is_empty()
            || entry
                .allowed_tools
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(tool_name));
        if !allowed_tool_match {
            skipped_tool.push(entry.skill_name);
            continue;
        }
        let matched_triggers = entry
            .triggers
            .iter()
            .filter(|trigger| {
                query_terms
                    .iter()
                    .any(|term| trigger.to_ascii_lowercase().contains(term))
                    || request
                        .query
                        .to_ascii_lowercase()
                        .contains(trigger.to_ascii_lowercase().as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        let lexical_matches = query_terms
            .iter()
            .filter(|term| {
                entry
                    .skill_name
                    .to_ascii_lowercase()
                    .contains(term.as_str())
                    || entry
                        .description
                        .to_ascii_lowercase()
                        .contains(term.as_str())
                    || entry
                        .tag_refs
                        .iter()
                        .any(|tag| tag.to_ascii_lowercase().contains(term.as_str()))
            })
            .count() as i64;
        let scope_bonus = 60_i64.saturating_sub(i64::from(entry_scope.precedence_rank));
        let score = context_skill_precedence_weight(entry.precedence)
            + scope_bonus
            + (matched_triggers.len() as i64 * 25)
            + (lexical_matches * 5)
            + i64::from(allowed_tool_match);
        push_unique_string(
            &mut scope_resolution_order,
            context_scope_layer_name(&entry_scope.layer).to_string(),
        );
        if !entry_scope.producer_agent_id.is_empty() {
            push_unique_string(&mut agent_producers, entry_scope.producer_agent_id.clone());
        }
        shared_graph_scopes.insert(entry_scope.shared_graph_scope.clone());
        selected.push(ContextSkillSelectionCandidate {
            skill_name: entry.skill_name,
            version: entry.version,
            owner_scope: entry.owner_scope,
            scope: entry_scope,
            precedence: entry.precedence,
            score,
            matched_triggers,
            allowed_tool_match,
            enabled: entry.enabled,
        });
    }
    selected.sort_by_key(|candidate| {
        (
            Reverse(candidate.score),
            Reverse(context_skill_precedence_weight(candidate.precedence)),
            candidate.scope.precedence_rank,
            candidate.skill_name.clone(),
        )
    });
    selected.truncate(request.limit.max(1));
    let status = if selected.is_empty() {
