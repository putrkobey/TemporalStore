// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// retrieve_context_pack_native, split from matrixark_rust_proxy_impl.rs (textually include!d;
// shares parent use-imports + flat scope; no use-statements or mod wrapper).

fn retrieve_context_pack_native(
    engine: &RecordStore,
    command: &RecordLogRequest,
) -> Result<Value, String> {
    let started = Instant::now();
    let request = command.record.clone().unwrap_or_else(|| json!({}));
    let query = request
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let query_terms: HashSet<String> = query
        .to_ascii_lowercase()
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| part.len() > 2)
        .map(str::to_string)
        .collect();
    let remote_budget = json_field(&request, &["local_budget", "remote_budget_tokens"])
        .and_then(Value::as_u64)
        .or_else(|| request.get("max_context_tokens").and_then(Value::as_u64))
        .unwrap_or(4000);
    let question_type = request
        .get("question_type")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| infer_native_question_type(&query).to_string());
    let max_refs = json_field(&request, &["ranking", "max_selected_refs"])
        .and_then(Value::as_u64)
        .unwrap_or(24)
        .max(1);
    let max_global_candidates = json_field(&request, &["ranking", "max_global_candidates"])
        .and_then(Value::as_u64)
        .unwrap_or(512)
        .max(1);
    let min_similarity_score = json_field(&request, &["ranking", "min_similarity_score"])
        .and_then(Value::as_f64)
        .unwrap_or(0.20)
        .clamp(0.0, 1.0);
    let budget_fill_policy = json_field(&request, &["ranking", "budget_fill_policy"])
        .and_then(Value::as_str)
        .filter(|policy| *policy == "quality_first" || *policy == "force_fill")
        .unwrap_or("quality_first")
        .to_string();
    let mut scan_command = command.clone();
    scan_command.scope = request
        .get("scope")
        .cloned()
        .or_else(|| command.scope.clone());
    scan_command.secondary_index_groups = request
        .get("secondary_index_groups")
        .and_then(Value::as_array)
        .map(|groups| {
            groups
                .iter()
                .map(|group| {
                    group
                        .as_array()
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Value::as_str)
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default()
                })
                .collect()
        })
        .or_else(|| command.secondary_index_groups.clone());
    if scan_command
        .record_types
        .as_ref()
        .map(Vec::is_empty)
        .unwrap_or(true)
    {
        scan_command.record_types = Some(vec![
            "context_compression_event".to_string(),
            "context_entity".to_string(),
            "context_event".to_string(),
            "context_segment".to_string(),
            "context_summary".to_string(),
            "resource_chunk".to_string(),
            "skill_section".to_string(),
            "context_index".to_string(),
        ]);
    }
    let scan_started = Instant::now();
    let scan = scan_matrixark_candidates(engine, &scan_command)?;
    let candidate_fetch_ms = scan_started.elapsed().as_secs_f64() * 1000.0;
    let records = scan
        .get("records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let scope_for_continuity = scan_command.scope.clone();
    // A borrow view: this path owns its records and goes on using them, and the inventory only
    // reads. One pointer per record, and nothing about this packer changes.
    let inventory_view: Vec<&Value> = records.iter().collect();
    let mut memory_inventory =
        native_retrieval_memory_inventory(&inventory_view, scope_for_continuity.as_ref());
    let cross_policy = parse_cross_session_policy(
        &request,
        scope_for_continuity.as_ref(),
        remote_budget,
        &question_type,
    );
    let source_role_budget_tokens = parse_source_role_budget_tokens(&request);
    let memory_selection_policy_budget_tokens =
        parse_memory_selection_policy_budget_tokens(&request);
    let extraction_phase_budget_tokens = parse_extraction_phase_budget_tokens(&request);
    let mut source_role_used_tokens: HashMap<String, u64> = HashMap::new();
    let mut source_role_selected_ref_counts: HashMap<String, u64> = HashMap::new();
    for role in source_role_budget_tokens.keys() {
        source_role_used_tokens.insert(role.clone(), 0);
        source_role_selected_ref_counts.insert(role.clone(), 0);
    }
    let mut memory_selection_policy_used_tokens: HashMap<String, u64> = HashMap::new();
    let mut memory_selection_policy_selected_ref_counts: HashMap<String, u64> = HashMap::new();
    for policy in memory_selection_policy_budget_tokens.keys() {
        memory_selection_policy_used_tokens.insert(policy.clone(), 0);
        memory_selection_policy_selected_ref_counts.insert(policy.clone(), 0);
    }
    let mut extraction_phase_used_tokens: HashMap<String, u64> = HashMap::new();
    let mut extraction_phase_selected_ref_counts: HashMap<String, u64> = HashMap::new();
    for phase in extraction_phase_budget_tokens.keys() {
        extraction_phase_used_tokens.insert(phase.clone(), 0);
        extraction_phase_selected_ref_counts.insert(phase.clone(), 0);
    }
    let mut raw_candidate_class_counts: HashMap<String, u64> = HashMap::new();
    let mut text_candidate_class_counts: HashMap<String, u64> = HashMap::new();
    let mut prepared_records = Vec::with_capacity(records.len());
    for record in records.iter() {
        let context_class = context_class_name(record);
        increment_class_count(&mut raw_candidate_class_counts, &context_class);
        let text = candidate_text(record);
        let tokens = token_estimate(&text);
        if !text.is_empty() {
            increment_class_count(&mut text_candidate_class_counts, &context_class);
        }
        prepared_records.push((record.clone(), context_class, text, tokens));
    }
    let score_started = Instant::now();
    let mut scored_candidate_class_counts: HashMap<String, u64> = HashMap::new();
    let mut score_threshold_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut scored: Vec<NativeScoredCandidate> = prepared_records
        .into_iter()
        .filter(|(record, _context_class, text, _tokens)| {
            matches!(
                record
                    .get("record_type")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
                "context_compression_event"
                    | "context_entity"
                    | "context_event"
                    | "context_segment"
                    | "context_summary"
                    | "resource_chunk"
                    | "skill_section"
            ) && !text.is_empty()
        })
        .filter_map(|(record, context_class, text, tokens)| {
            let mut score = sparse_query_score(&query_terms, &text);
            if matches!(
                record.get("record_type").and_then(Value::as_str),
                Some("context_entity")
            ) {
                score += 0.08;
            }
            if matches!(
                record.get("record_type").and_then(Value::as_str),
                Some("context_compression_event")
            ) {
                score += 0.06;
            }
            let session_continuity =
                session_continuity_status(&record, scope_for_continuity.as_ref());
            let continuity_boost_value =
                continuity_boost(&record, &context_class, &session_continuity);
            score += continuity_boost_value;
            let cross_session_rerank_boost_value = cross_session_rerank_boost(
                &record,
                &context_class,
                &session_continuity,
                &question_type,
            );
            score += cross_session_rerank_boost_value;
            score += type_priority_boost(&record, &context_class, &question_type);
            if score >= min_similarity_score {
                increment_class_count(&mut scored_candidate_class_counts, &context_class);
                Some(NativeScoredCandidate {
                    score,
                    record,
                    text,
                    tokens,
                    context_class,
                    session_continuity,
                    continuity_boost_value,
                    cross_session_rerank_boost_value,
                })
            } else {
                increment_class_count(&mut score_threshold_dropped_class_counts, &context_class);
                None
            }
        })
        .collect();
    let score_ms = score_started.elapsed().as_secs_f64() * 1000.0;
    scored.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if scored.len() > max_global_candidates as usize {
        scored.truncate(max_global_candidates as usize);
    }
    let mut selected = Vec::new();
    let mut selected_signatures: HashSet<String> = HashSet::new();
    let mut selected_counts: HashMap<String, u64> = HashMap::new();
    let mut selected_nodes: HashSet<u64> = HashSet::new();
    let mut dropped_over_budget = 0_u64;
    let mut dropped_cross_budget = 0_u64;
    let mut dropped_cross_session_cap = 0_u64;
    let mut dropped_cross_candidate_cap = 0_u64;
    let mut dropped_source_role_budget = 0_u64;
    let mut dropped_memory_selection_policy_budget = 0_u64;
    let mut dropped_extraction_phase_budget = 0_u64;
    let mut dropped_low_score = 0_u64;
    let mut dropped_duplicate_ref = 0_u64;
    let mut dropped_policy_ref = 0_u64;
    let mut dropped_stale_ref = 0_u64;
    let mut budget_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut policy_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut duplicate_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut cross_policy_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut cross_low_score_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut cross_cap_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut source_role_budget_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut memory_selection_policy_budget_dropped_class_counts: HashMap<String, u64> =
        HashMap::new();
    let mut extraction_phase_budget_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut selected_class_counts: HashMap<String, u64> = HashMap::new();
    let mut dropped_ref_type_counts: HashMap<String, u64> = HashMap::new();
    let mut dropped_ref_type_token_counts: HashMap<String, u64> = HashMap::new();
    let mut dropped_over_budget_tokens = 0_u64;
    let mut dropped_cross_budget_tokens = 0_u64;
    let mut dropped_cross_session_cap_tokens = 0_u64;
    let mut dropped_cross_candidate_cap_tokens = 0_u64;
    let mut dropped_source_role_budget_tokens = 0_u64;
    let mut dropped_memory_selection_policy_budget_tokens = 0_u64;
    let mut dropped_extraction_phase_budget_tokens = 0_u64;
    let mut dropped_low_score_tokens = 0_u64;
    let mut dropped_duplicate_ref_tokens = 0_u64;
    let mut dropped_policy_ref_tokens = 0_u64;
    let mut dropped_stale_ref_tokens = 0_u64;
    let mut dropped_ref_details: Vec<Value> = Vec::new();
    let mut cross_used_tokens = 0_u64;
    let mut cross_selected_refs = 0_u64;
    let mut entity_bridge_selected_refs = 0_u64;
    let mut selected_cross_sessions: HashSet<String> = HashSet::new();
    let mut used_tokens = 0_u64;
    let has_scored_event_candidate = scored.iter().any(|candidate| candidate.context_class == "event");
    let summary_allowed_for_question = matches!(
        question_type.as_str(),
        "broad" | "broad_exploration" | "exploration" | "profile_memory"
    );
    let current_state_query = matches!(
        question_type.as_str(),
        "current_state" | "latest" | "profile_memory"
    );
    let (profile_by_entity, profile_by_source_entity_hash) = if current_state_query {
        profile_shadow_maps(&scored)
    } else {
        (HashMap::new(), HashMap::new())
    };
    for candidate in scored {
        if selected.len() as u64 >= max_refs {
            break;
        }
        let NativeScoredCandidate {
            score,
            record,
            text,
            tokens,
            context_class,
            session_continuity,
            continuity_boost_value,
            cross_session_rerank_boost_value,
        } = candidate;
        let candidate_for_shadow = NativeScoredCandidate {
            score,
            record: record.clone(),
            text: text.clone(),
            tokens,
            context_class: context_class.clone(),
            session_continuity: session_continuity.clone(),
            continuity_boost_value,
            cross_session_rerank_boost_value,
        };
        if current_state_query {
            if let Some(profile_shadow) = profile_shadow_for_candidate(
                &candidate_for_shadow,
                &profile_by_entity,
                &profile_by_source_entity_hash,
            ) {
                dropped_stale_ref += 1;
                dropped_stale_ref_tokens += tokens;
                increment_class_count(&mut dropped_ref_type_counts, &context_class);
                increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
                dropped_ref_details.push(native_dropped_ref_detail(
                    &record,
                    &text,
                    &context_class,
                    "stale",
                    tokens,
                    Some(profile_shadow),
                ));
                continue;
            }
        }
        if used_tokens + tokens > remote_budget {
            dropped_over_budget += 1;
            dropped_over_budget_tokens += tokens;
            increment_class_count(&mut budget_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if !is_serving_selected_ref_class(&context_class) {
            dropped_policy_ref += 1;
            dropped_policy_ref_tokens += tokens;
            increment_class_count(&mut policy_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if context_class == "summary" && has_scored_event_candidate && !summary_allowed_for_question {
            dropped_policy_ref += 1;
            dropped_policy_ref_tokens += tokens;
            increment_class_count(&mut policy_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        let is_cross_session = session_continuity == "cross_session";
        let record_type = record
            .get("record_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        let is_entity_bridge = is_cross_session && context_class == "entity";
        let is_cross_session_raw_evidence =
            is_cross_session && matches!(record_type, "context_event" | "context_segment");
        let cross_key = if is_cross_session {
            cross_session_key(&record)
        } else {
            String::new()
        };
        if is_cross_session && !cross_policy.enabled {
            dropped_cross_budget += 1;
            dropped_cross_budget_tokens += tokens;
            increment_class_count(&mut cross_policy_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if is_cross_session && cross_policy.min_score > 0.0 && score < cross_policy.min_score {
            dropped_low_score += 1;
            dropped_low_score_tokens += tokens;
            increment_class_count(&mut cross_low_score_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if is_cross_session_raw_evidence
            && cross_policy.raw_evidence_min_score > 0.0
            && score < cross_policy.raw_evidence_min_score
        {
            dropped_low_score += 1;
            dropped_low_score_tokens += tokens;
            increment_class_count(&mut cross_low_score_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if is_cross_session
            && cross_policy.max_candidates > 0
            && cross_selected_refs >= cross_policy.max_candidates
        {
            dropped_cross_candidate_cap += 1;
            dropped_cross_candidate_cap_tokens += tokens;
            increment_class_count(&mut cross_cap_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if is_cross_session
            && cross_policy.max_sessions > 0
            && !selected_cross_sessions.contains(&cross_key)
            && selected_cross_sessions.len() as u64 >= cross_policy.max_sessions
        {
            dropped_cross_session_cap += 1;
            dropped_cross_session_cap_tokens += tokens;
            increment_class_count(&mut cross_cap_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if is_cross_session
            && cross_policy.budget_tokens > 0
            && cross_used_tokens + tokens > cross_policy.budget_tokens
            && !(is_entity_bridge
                && entity_bridge_selected_refs < cross_policy.min_entity_bridge_refs)
        {
            dropped_cross_budget += 1;
            dropped_cross_budget_tokens += tokens;
            increment_class_count(&mut cross_cap_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        let candidate_source_roles = source_role_names(&record);
        let capped_roles: Vec<String> = candidate_source_roles
            .iter()
            .filter(|role| {
                source_role_budget_tokens
                    .get(*role)
                    .map(|budget| {
                        source_role_used_tokens.get(*role).copied().unwrap_or(0) + tokens > *budget
                    })
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        if !capped_roles.is_empty() {
            dropped_source_role_budget += 1;
            dropped_source_role_budget_tokens += tokens;
            increment_class_count(&mut source_role_budget_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            let mut detail = native_dropped_ref_detail(
                &record,
                &text,
                &context_class,
                "source_role_budget",
                tokens,
                None,
            );
            if let Some(object) = detail.as_object_mut() {
                object.insert(
                    "source_role_budget_capped_roles".to_string(),
                    json!(capped_roles),
                );
            }
            dropped_ref_details.push(detail);
            continue;
        }
        let candidate_memory_selection_policies = memory_selection_policy_names(&record);
        let capped_memory_selection_policies: Vec<String> = candidate_memory_selection_policies
            .iter()
            .filter(|policy| {
                memory_selection_policy_budget_tokens
                    .get(*policy)
                    .map(|budget| {
                        memory_selection_policy_used_tokens
                            .get(*policy)
                            .copied()
                            .unwrap_or(0)
                            + tokens
                            > *budget
                    })
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        if !capped_memory_selection_policies.is_empty() {
            dropped_memory_selection_policy_budget += 1;
            dropped_memory_selection_policy_budget_tokens += tokens;
            increment_class_count(
                &mut memory_selection_policy_budget_dropped_class_counts,
                &context_class,
            );
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            let mut detail = native_dropped_ref_detail(
                &record,
                &text,
                &context_class,
                "memory_selection_policy_budget",
                tokens,
                None,
            );
            if let Some(object) = detail.as_object_mut() {
                object.insert(
                    "memory_selection_policy_budget_capped_policies".to_string(),
                    json!(capped_memory_selection_policies),
                );
            }
            dropped_ref_details.push(detail);
            continue;
        }
