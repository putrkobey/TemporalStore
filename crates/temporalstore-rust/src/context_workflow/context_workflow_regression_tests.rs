// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;


fn test_engine() -> TemporalEngine {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine
}

// shared-corpus: context_retrieval_batch_node_summary_lookup
#[test]
fn context_get_nodes_batches_summary_lookup_for_retrieval() {
    let engine = test_engine();
    let tenant_hash = 91;
    for node_hash in [11, 22] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash,
                node: Box::new(ContextNode {
                    node_hash,
                    parent_hash: 0,
                    kind: 0,
                    canonical_name: format!("node-{node_hash}"),
                    l0: format!("l0 summary for node {node_hash}"),
                    status: 0,
                    last_event_time_ms: 1_000 + node_hash,
                    l1_ref: format!("l1 summary for node {node_hash}"),
                    raw_metadata_ref: format!("source://node/{node_hash}"),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
                    summary_vector: Vec::new(),
                    summary_vector_valid_from_ms: 0,
                    summary_vector_model_hash: 0,
                }),
            },
        });
        assert!(response.status.ok, "{:?}", response.status);
    }

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNodes {
            tenant_hash,
            node_hashes: vec![11, 22, 33],
        },
    });

    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::ContextNodes { nodes } => {
            assert_eq!(nodes.len(), 2);
            assert_eq!(nodes[0].node_hash, 11);
            assert_eq!(nodes[1].node_hash, 22);
            assert!(nodes[0].l0.contains("node 11"));
            // l1_ref is a deprecated hot-schema field dropped from the persisted wire
            // (its L1 summary now lives in ContextSummary records); assert the
            // surviving l0 summary content for node 22 instead.
            assert!(nodes[1].l0.contains("node 22"));
        }
        other => panic!("unexpected response: {other:?}"),
    }
}

// shared-corpus: context_retrieval_qa_synonym_ranking
#[test]
fn context_relevance_ranks_qa_synonyms_and_phrases() {
    let relevant =
        "Checkout incident: payment safety score spiked after a backend dependency outage.";
    let distractor = "Support ticket: user asked for help updating a notification preference.";

    assert!(context_query_matches("payment fraud score", relevant));
    assert!(
        context_relevance_score("payment fraud score", relevant)
            > context_relevance_score("payment fraud score", distractor)
    );
    assert!(
        context_relevance_score("service timeline outage", relevant)
            > context_relevance_score("service timeline outage", distractor)
    );
    assert!(context_relevance_score("payment fraud score", relevant) >= 100);

    let updated_memory = benchmark_context_body(7, 2, 4);
    let stale_memory = benchmark_context_body(1, 2, 1);
    assert!(context_query_matches(
        "latest customer preference update topic 2",
        &updated_memory
    ));
    assert!(
        context_relevance_score("latest customer preference update topic 2", &updated_memory)
            > context_relevance_score("latest customer preference update topic 2", &stale_memory)
    );

    let locomo_memory = "During the latest conversation, Alice replaced her office preference with the downtown location after the billing issue was resolved.";
    let locomo_stale =
        "Earlier conversation memory: Alice preferred the airport office before the later change.";
    assert!(context_query_matches(
        "What is Alice's current office choice after the payment problem?",
        locomo_memory
    ));
    assert!(
        context_relevance_score(
            "What is Alice's current office choice after the payment problem?",
            locomo_memory
        ) > context_relevance_score(
            "What is Alice's current office choice after the payment problem?",
            locomo_stale
        )
    );
    let debug_terms = context_query_terms("Where is Alice currently located?");
    let debug_groups = context_query_secondary_index_filter_groups(&debug_terms);
    assert_eq!(context_query_question_type(&debug_terms), "current_state");
    assert!(debug_groups
        .iter()
        .any(|group| group.iter().any(|term| term == "entity_type:location")));
    assert!(debug_groups
        .iter()
        .any(|group| group.iter().any(|term| term == "event_type:status_update")));

    let longmem_memory = "Support follow-up: the user sent messages across sessions and the helpdesk agent changed the notification setting during the most recent chat.";
    assert!(context_query_matches(
        "Which preference was updated in the recent multi session messages?",
        longmem_memory
    ));

    let corrected_memory = "Latest correction: Jordan no longer avoids almonds; Jordan should avoid peanuts now because of a new food restriction.";
    let stale_food_memory =
        "Earlier chat: Jordan said almonds were the only snack to avoid and peanuts were fine.";
    assert!(context_query_matches(
        "What snack should Jordan avoid now after the correction?",
        corrected_memory
    ));
    assert!(
        context_relevance_score(
            "What snack should Jordan avoid now after the correction?",
            corrected_memory
        ) > context_relevance_score(
            "What snack should Jordan avoid now after the correction?",
            stale_food_memory
        )
    );

    let medication_memory = "In the later session Morgan said to remember lisinopril, the blood pressure medication, before the doctor appointment.";
    let stale_clinic_memory =
        "A previous clinic message mentioned bringing an insurance card to the physician visit.";
    assert!(context_query_matches(
        "Which medication did Morgan say to remember before the doctor appointment?",
        medication_memory
    ));
    assert!(
        context_relevance_score(
            "Which medication did Morgan say to remember before the doctor appointment?",
            medication_memory
        ) > context_relevance_score(
            "Which medication did Morgan say to remember before the doctor appointment?",
            stale_clinic_memory
        )
    );

    let hobby_switch = "Later update: Priya cancelled guitar lessons and switched to a pottery class instead for the spring session.";
    let stale_hobby = "Earlier conversation: Priya planned guitar lessons and had not picked a replacement hobby yet.";
    assert!(context_query_matches(
        "Which hobby did Priya switch to after cancelling guitar lessons?",
        hobby_switch
    ));
    assert!(
        context_relevance_score(
            "Which hobby did Priya switch to after cancelling guitar lessons?",
            hobby_switch
        ) > context_relevance_score(
            "Which hobby did Priya switch to after cancelling guitar lessons?",
            stale_hobby
        )
    );

    let current_backup =
            "Most recent staffing update: Sam moved teams, so Riley became the backup contact for payment escalation now.";
    let stale_backup =
        "Old support note: Sam was the backup contact for payment escalation before the team move.";
    assert!(context_query_matches(
        "Who is the backup contact now after Sam moved teams?",
        current_backup
    ));
    assert!(
        context_relevance_score(
            "Who is the backup contact now after Sam moved teams?",
            current_backup
        ) > context_relevance_score(
            "Who is the backup contact now after Sam moved teams?",
            stale_backup
        )
    );

    let cafe_recommendation = "Later chat: Omar recommended the quiet riverside cafe, and Nina booked it after the conference.";
    let stale_cafe = "Earlier conversation: Nina wanted to book a cafe after the conference but had not chosen one yet.";
    assert!(context_query_matches(
        "Who recommended the cafe that Nina booked after the conference?",
        cafe_recommendation
    ));
    assert!(
        context_relevance_score(
            "Who recommended the cafe that Nina booked after the conference?",
            cafe_recommendation
        ) > context_relevance_score(
            "Who recommended the cafe that Nina booked after the conference?",
            stale_cafe
        )
    );

    let project_suggestion = "Later planning note: Dana suggested the observability dashboard because the team needed better benchmark traces, so Lee picked that project.";
    let stale_project = "Initial planning thread: Lee considered a search cleanup project and had not chosen the final work item.";
    assert!(context_query_matches(
        "Which project did Lee pick because Dana suggested it during planning?",
        project_suggestion
    ));
    assert!(
        context_relevance_score(
            "Which project did Lee pick because Dana suggested it during planning?",
            project_suggestion
        ) > context_relevance_score(
            "Which project did Lee pick because Dana suggested it during planning?",
            stale_project
        )
    );

    let rescheduled_appointment = "Latest calendar update: Maya rescheduled the dentist appointment to Thursday at 3pm after the clinic called.";
    let stale_appointment =
        "Earlier memory: Maya had a dentist appointment scheduled for Tuesday morning.";
    assert!(context_query_matches(
        "When is Maya's dentist appointment after it was rescheduled?",
        rescheduled_appointment
    ));
    assert!(
        context_relevance_score(
            "When is Maya's dentist appointment after it was rescheduled?",
            rescheduled_appointment
        ) > context_relevance_score(
            "When is Maya's dentist appointment after it was rescheduled?",
            stale_appointment
        )
    );

    let updated_deadline = "Calendar update: the report deadline moved to June 24 so the benchmark review could finish first.";
    let stale_deadline =
        "Old planning note: the report deadline was June 17 before the later schedule change.";
    assert!(context_query_matches(
        "What is the new report deadline after the calendar update?",
        updated_deadline
    ));
    assert!(
        context_relevance_score(
            "What is the new report deadline after the calendar update?",
            updated_deadline
        ) > context_relevance_score(
            "What is the new report deadline after the calendar update?",
            stale_deadline
        )
    );

    let updated_guest_count =
        "Final RSVP update: Sofia confirmed 7 guests for dinner after two neighbors joined.";
    let stale_guest_count =
        "Earlier dinner plan: Sofia expected 4 guests before the final RSVP update.";
    assert!(context_query_matches(
        "How many guests did Sofia confirm after the dinner update?",
        updated_guest_count
    ));
    assert!(
        context_relevance_score(
            "How many guests did Sofia confirm after the dinner update?",
            updated_guest_count
        ) > context_relevance_score(
            "How many guests did Sofia confirm after the dinner update?",
            stale_guest_count
        )
    );

    let updated_risk_score =
            "Latest fraud review: the checkout risk score was updated to 87 after the payment incident escalated.";
    let stale_risk_score =
            "Earlier fraud review: the checkout risk score was 42 before the payment incident escalated.";
    assert!(context_query_matches(
        "What risk score was recorded after the latest fraud review?",
        updated_risk_score
    ));
    assert!(
        context_relevance_score(
            "What risk score was recorded after the latest fraud review?",
            updated_risk_score
        ) > context_relevance_score(
            "What risk score was recorded after the latest fraud review?",
            stale_risk_score
        )
    );

    let new_roommate =
            "After the move, Emma said her new roommate is named Lena and they share the corner apartment.";
    let old_roommate =
        "Earlier chat: Emma's roommate was called Nora before Emma moved apartments.";
    assert!(context_query_matches(
        "What is Emma's roommate's name after the move?",
        new_roommate
    ));
    assert!(
        context_relevance_score(
            "What is Emma's roommate's name after the move?",
            new_roommate
        ) > context_relevance_score(
            "What is Emma's roommate's name after the move?",
            old_roommate
        )
    );

    let new_pet = "Latest pet update: the newly adopted dog is named Miso and needs evening walks.";
    let old_pet = "Old profile note: the family dog was called Pepper in a previous home.";
    assert!(context_query_matches(
        "What is the dog's name in the latest pet update?",
        new_pet
    ));
    assert!(
        context_relevance_score("What is the dog's name in the latest pet update?", new_pet)
            > context_relevance_score("What is the dog's name in the latest pet update?", old_pet)
    );
}

// shared-corpus: context_retrieval_precomputed_query_plan_parity
#[test]
fn context_query_plan_matches_legacy_match_and_score_helpers() {
    let cases = [
        (
            "What is Alice's current office choice after the payment problem?",
            "During the latest conversation, Alice replaced her office preference with the downtown location after the billing issue was resolved.",
        ),
        (
            "Which medication did Morgan say to remember before the doctor appointment?",
            "In the later session Morgan said to remember lisinopril, the blood pressure medication, before the doctor appointment.",
        ),
        (
            "How many total checkout retries happened across sessions?",
            "Across three sessions the support agent counted 12 checkout retries, 4 payment errors, and 2 successful follow-ups.",
        ),
        (
            "Which hobby did Priya switch to after cancelling guitar lessons?",
            "Later update: Priya cancelled guitar lessons and switched to a pottery class instead for the spring session.",
        ),
    ];

    for (query, text) in cases {
        let plan = context_query_plan(query);
        assert_eq!(
            context_query_matches(query, text),
            context_query_matches_plan(&plan, text)
        );
        assert_eq!(
            context_relevance_score(query, text),
            context_relevance_score_plan(&plan, text)
        );
        assert!(!plan.terms.is_empty());
        assert_eq!(
            plan.term_groups,
            context_query_term_groups_from_terms(&plan.terms)
        );
    }
}

#[test]
fn context_extract_gates_l1_for_thin_sources() {
    let engine = test_engine();
    // Thin single-sentence source: L1 is gated -> L0 only, 2 embeddings.
    let thin = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 5,
            source_kind: ContextSourceKind::Chat,
            source_id: "thin".to_string(),
            title: "note".to_string(),
            body: "Checkout failed once.".to_string(),
            timestamp_ms: 1,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(thin.status.ok, "{:?}", thin.status);
    assert!(!thin.l0.is_empty());
    assert!(thin.l1.is_empty(), "thin source should skip L1: {:?}", thin.l1);
    assert!(thin.node.l1_ref.is_empty());
    assert_eq!(thin.embedding_generation.requested_vector_count, 2);

    // Richer multi-sentence source: L1 is warranted -> L0 + L1, still 2 embeddings. The node
    // embeds `l1` and the level-2 summary IS `l1`, so that string is encoded once and shared;
    // the vector count is the same as the thin path and only the summaries differ.
    let rich = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 5,
            source_kind: ContextSourceKind::Chat,
            source_id: "rich".to_string(),
            title: "note".to_string(),
            body: "Checkout failed during payment. The risk score spiked sharply. The fraud team paused the account."
                .to_string(),
            timestamp_ms: 2,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(rich.status.ok, "{:?}", rich.status);
    assert!(!rich.l1.is_empty());
    assert_eq!(
        rich.embedding_generation.requested_vector_count, 2,
        "emitting L1 must not cost a second encode of the same string"
    );
}

#[test]
fn context_extract_stores_embedding_vectors_on_the_records_themselves() {
    // Step 2 of the embedding fold populates event and summary records with their own vector,
    // which is the only place they live. Nothing else READS the inline
    // field yet, so without this assertion "population" could be silently writing empty vectors
    // and every existing test would still pass.
    let engine = test_engine();
    let report = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 77,
            source_kind: ContextSourceKind::Chat,
            source_id: "inline-vector".to_string(),
            title: "note".to_string(),
            body: "Checkout failed during payment. The risk score spiked sharply.                    The fraud team paused the account."
                .to_string(),
            timestamp_ms: 7,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(report.status.ok, "{:?}", report.status);
    // Rich body -> L1 is warranted, so both vectors exist: node text and event text. The L1
    // summary takes the node's vector rather than asking for one of its own.
    assert_eq!(report.embedding_generation.requested_vector_count, 2);

    let events = match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryEvents {
                tenant_hash: 77,
                node_hash: report.node.node_hash,
                start_time_ms: 0,
                end_time_ms: 4_000_000_000_000,
                limit: None,
                max_scan: None,
                current_valid_only: false,
                as_of_ms: 0,
                kinds: Vec::new(),
                statuses: Vec::new(),
                min_confidence: 0.0,
                min_importance: 0.0,
            },
        })
        .response
    {
        CommandResponse::ContextEvents { events, .. } => events,
        other => panic!("expected events, got {other:?}"),
    };
    let stored = events.first().expect("the extract wrote an event");
    assert!(
        !stored.vector.is_empty(),
        "event was stored without its inline vector"
    );

    let summaries = match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQuerySummaries {
                tenant_hash: 77,
                node_hash: report.node.node_hash,
                level: 1,
                as_of_ms: 4_000_000_000_000,
                limit: None,
            },
        })
        .response
    {
        CommandResponse::ContextSummaries { summaries, .. } => summaries,
        other => panic!("expected summaries, got {other:?}"),
    };
    assert!(!summaries.is_empty(), "the extract wrote no summaries");
    for summary in &summaries {
        assert!(
            !summary.vector.is_empty(),
            "summary level {} was stored without its inline vector",
            summary.level
        );
    }
}

#[test]
fn a_node_and_its_level_two_summary_share_one_embedding() {
    // The node is embedded from `l1`, and the level-2 summary's text IS `l1`. One string, so one
    // request to the encoder, and both owners take the vector it returns.
    //
    // The count is the load-bearing half. Comparing the two vectors alone would pass just as
    // happily if the provider had been asked twice and answered the same way both times, which is
    // precisely the duplicate this removes -- so the request count is asserted first, and the
    // equal vectors afterwards say the shared one actually reached both records.
    let engine = test_engine();
    let report = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 81,
            source_kind: ContextSourceKind::Chat,
            source_id: "shared-vector".to_string(),
            title: "note".to_string(),
            body: "Checkout failed during payment. The risk score spiked sharply. The fraud team paused the account."
                .to_string(),
            timestamp_ms: 11,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(report.status.ok, "{:?}", report.status);
    assert!(
        !report.l1.is_empty(),
        "this body must warrant L1 or the shared case is never reached"
    );
    assert_eq!(
        report.embedding_generation.requested_vector_count, 2,
        "the node text and the event body; `l1` is encoded once, not once per owner"
    );

    let summaries = match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQuerySummaries {
                tenant_hash: 81,
                node_hash: report.node.node_hash,
                level: 2,
                as_of_ms: 4_000_000_000_000,
                limit: None,
            },
        })
        .response
    {
        CommandResponse::ContextSummaries { summaries, .. } => summaries,
        other => panic!("expected summaries, got {other:?}"),
    };
    let summary = summaries.first().expect("the extract wrote a level-2 summary");
    assert_eq!(summary.text, report.l1, "the summary that embeds the node's own text");

    // The NODE as stored, not as reported. The summary above came back through a page, and every
    // stored vector form rounds -- four decimal places for the uniformly-scaled encoding that is
    // on by default, a per-vector scale for the int8 one -- so the value in the report is not the
    // value on disk. Comparing the two would not be asserting that one vector reached both
    // owners; it would be asserting that the encoding is lossless, which it is not and is not
    // meant to be.
    //
    // Reading the node back is also the stronger claim of the two: it says the shared vector
    // reached the durable record, not merely the report that the extract returned.
    let node = match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextGetNode {
                tenant_hash: 81,
                node_hash: report.node.node_hash,
            },
        })
        .response
    {
        CommandResponse::ContextNode { node: Some(node), .. } => node,
        other => panic!("expected the node back, got {other:?}"),
    };
    assert!(
        !node.vector.is_empty(),
        "an empty node vector would make the comparison below vacuous"
    );
    assert_eq!(
        summary.vector, node.vector,
        "the one vector `l1` produced belongs to both the node and its level-2 summary"
    );
}

#[test]
fn an_ingested_summary_records_the_encoder_that_embedded_it() {
    // The stamp the summary guard reads. Level 2 is the only summary level retrieval scores, so
    // a vector written there without its encoder recorded is one the guard must wave through --
    // and the check quietly stops applying to everything written from then on. Asserted on the
    // records as they come back off the engine, not on the report, because the record is what a
    // later retrieve actually reads.
    let engine = test_engine();
    let provider = ContextModelProviderConfig {
        model: "deepseek-chat".to_string(),
        embedding_model: "intfloat/multilingual-e5-large".to_string(),
        ..ContextModelProviderConfig::default()
    };
    let expected = context_embedding_model_hash(&provider.embedding_model);
    assert_ne!(
        expected,
        context_embedding_model_hash(&provider.model),
        "chat and embedding models must hash apart, or a chat-model stamp would pass this"
    );
    assert_ne!(0, expected, "an unknown stamp would be waved through, not checked");

    let report = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 79,
            source_kind: ContextSourceKind::Chat,
            source_id: "summary-encoder".to_string(),
            title: "note".to_string(),
            body: "Checkout failed during payment. The risk score spiked sharply.                    The fraud team paused the account."
                .to_string(),
            timestamp_ms: 7,
            provider: provider.clone(),
        },
    );
    assert!(report.status.ok, "{:?}", report.status);
    // Rich body -> L1 is warranted, so the level-2 summary exists and carries a vector -- the
    // node's own, shared rather than requested a second time.
    assert_eq!(report.embedding_generation.requested_vector_count, 2);

    for level in [1u32, 2] {
        let summaries = match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextQuerySummaries {
                    tenant_hash: 79,
                    node_hash: report.node.node_hash,
                    level,
                    as_of_ms: 4_000_000_000_000,
                    limit: None,
                },
            })
            .response
        {
            CommandResponse::ContextSummaries { summaries, .. } => summaries,
            other => panic!("expected summaries, got {other:?}"),
        };
        assert!(
            !summaries.is_empty(),
            "the extract wrote no level {level} summary"
        );
        for summary in &summaries {
            assert!(
                !summary.vector.is_empty(),
                "level {level} summary has no vector, so there is nothing to identify"
            );
            assert_eq!(
                expected, summary.embedding_model_hash,
                "level {level} summary must record the encoder that produced its vector"
            );
        }
    }
}

// shared-corpus: context_compression_secondary_index_query_debug_flow
#[test]
fn context_workflow_extracts_retrieves_and_injects_mock_context() {
    let engine = test_engine();
    let extract = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 42,
            source_kind: ContextSourceKind::Incident,
            source_id: "INC-1".to_string(),
            title: "Checkout incident".to_string(),
            body: "Customer checkout failed. Payment risk score spiked.".to_string(),
            timestamp_ms: 1_000,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(extract.status.ok);
    assert!(extract.node_uri.starts_with("tsctx://tenant/42/node/"));

    let retrieve = retrieve_context(
        &engine,
        ContextRetrieveRequest {
            shard_id: 1,
            tenant_hash: 42,
            node_hashes: vec![extract.node.node_hash],
            query: "checkout".to_string(),
            start_time_ms: 0,
            end_time_ms: 2_000,
            max_events: 8,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: default_tiers(),
            max_summary_nodes: 32,
            max_event_nodes: 16,
            prefer_current_agent: false,
            current_agent_scope_key: "agent:codex".to_string(),
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(retrieve.status.ok);
    assert!(retrieve
        .blocks
        .iter()
        .any(|block| block.tier == ContextTier::L0));
    assert!(retrieve
        .blocks
        .iter()
        .any(|block| block.tier == ContextTier::L2));
    assert!(
        retrieve
            .query_understanding_debug
            .tree_traversal_summary
            .enabled
    );
    assert_eq!(
        retrieve
            .query_understanding_debug
            .tree_traversal_summary
            .summary_embedding_candidate_count,
        1
    );
    assert_eq!(
        retrieve
            .query_understanding_debug
            .tree_traversal_summary
            .summary_embedding_selected_count,
        1
    );
    assert_eq!(
        retrieve
            .query_understanding_debug
            .tree_traversal_summary
            .query_embedding_dimension,
        16
    );
    assert!(retrieve
        .query_understanding_debug
        .tree_traversal_summary
        .summary_embeddings
        .iter()
        .any(|entry| entry.starts_with("node:") && entry.contains(":score:")));
    assert!(!retrieve
        .query_understanding_debug
        .verbose_filter_groups
        .is_empty());
    assert!(retrieve
        .query_understanding_debug
        .verbose_filter_groups
        .iter()
        .any(|group| group.candidate_count > 0 && group.matched_count > 0));
    assert!(!retrieve.query_understanding_debug.selected_refs.is_empty());
    assert_eq!(retrieve.query_understanding_debug.selected_refs[0].rank, 1);
    assert!(retrieve
        .query_understanding_debug
        .selected_refs
        .iter()
        .any(|selected| selected
            .matched_filter_groups
            .iter()
            .any(|group_id| group_id.starts_with("filter_group_"))));
    assert!(retrieve.parity.pipeline_ready);
    assert!(retrieve.parity.native_context_models_ready);
    assert!(retrieve.parity.reference_tiers_ready);
    assert!(retrieve.parity.shared_store_sync_ready);
    assert!(retrieve.parity.raft_read_ready);

    let inject = inject_context(
        &engine,
        ContextInjectRequest {
            retrieve: ContextRetrieveRequest {
                shard_id: 1,
                tenant_hash: 42,
                node_hashes: vec![extract.node.node_hash],
                query: "checkout".to_string(),
                start_time_ms: 0,
                end_time_ms: 2_000,
                max_events: 8,
                min_confidence: 0.0,
                min_importance: 0.0,
                tiers: default_tiers(),
                max_summary_nodes: 32,
                max_event_nodes: 16,
                prefer_current_agent: false,
                current_agent_scope_key: "agent:codex".to_string(),
                provider: ContextModelProviderConfig::default(),
            },
            prompt: "Explain current risk.".to_string(),
            session_hash: 7,
            query_id: "q1".to_string(),
            max_prompt_tokens: 128,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(inject.status.ok);
    assert!(inject.injected_prompt.contains("<context>"));
    assert!(!inject.audit.selected_refs.is_empty());
}

// shared-corpus: context_query_debug_filter_group_parity
#[test]
fn context_query_debug_reports_filter_groups_drops_and_injection_order() {
    let engine = test_engine();
    let relevant = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 4242,
            source_kind: ContextSourceKind::Incident,
            source_id: "INC-payment-1".to_string(),
            title: "Payment risk incident".to_string(),
            body:
                "Latest checkout update: payment risk score moved to 87 after the gateway timeout."
                    .to_string(),
            timestamp_ms: 10_000,
            provider: ContextModelProviderConfig::default(),
        },
    );
    let unrelated = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 4242,
            source_kind: ContextSourceKind::Chat,
            source_id: "CHAT-preference-1".to_string(),
            title: "Notification preference".to_string(),
            body: "Support chat: the user changed the email notification preference to weekly."
                .to_string(),
            timestamp_ms: 11_000,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(relevant.status.ok, "{:?}", relevant.status);
    assert!(unrelated.status.ok, "{:?}", unrelated.status);

    let report = retrieve_context(
        &engine,
        ContextRetrieveRequest {
            shard_id: 1,
            tenant_hash: 4242,
            node_hashes: vec![relevant.node.node_hash, unrelated.node.node_hash],
            query: "What is the latest checkout payment risk score?".to_string(),
            start_time_ms: 0,
            end_time_ms: 20_000,
            max_events: 8,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: vec![ContextTier::L0, ContextTier::L1, ContextTier::L2],
            max_summary_nodes: 32,
            max_event_nodes: 16,
            prefer_current_agent: false,
            current_agent_scope_key: "agent:codex".to_string(),
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(report.status.ok, "{:?}", report.status);
    let debug = &report.query_understanding_debug;
    assert_eq!(debug.debug_schema, "matrixark_context_query_debug_v1");
    assert_ne!(debug.query_hash, 0);
    assert!(debug
        .normalized_query_terms
        .contains(&"checkout".to_string()));
    assert_eq!(debug.question_type, "current_state");
    assert!(debug.filter_group_summary.total_groups >= 2);
    assert!(debug.filter_group_summary.secondary_index_group_count >= 1);
    assert!(debug.filter_group_summary.total_candidate_count >= 2);
    assert!(debug.filter_group_summary.total_matched_count >= 1);
    assert!(debug.filter_group_summary.total_dropped_count >= 1);
    assert!(debug.candidates_passing_prefilter >= 1);
    assert!(debug.candidates_dropped_before_scoring >= 1);
    assert!(debug
        .verbose_filter_groups
        .iter()
        .any(|group| group.group_kind == "secondary_index_prefilter"
            && group.matched_count > 0
            && group.dropped_count > 0
            && group
                .candidate_decisions
                .iter()
                .any(|decision| decision.decision == "matched"
                    && decision.reason == "matched_filter_group"
                    && !decision.matched_terms.is_empty())
            && group
                .candidate_decisions
                .iter()
                .any(|decision| decision.decision == "dropped"
                    && decision.reason == "secondary_index_prefilter_miss"
                    && decision.matched_terms.is_empty())));
    assert!(debug.prefilter_candidate_sample.iter().any(|candidate| {
        !candidate.passes_secondary_index_prefilter
            && candidate.drop_reason == "secondary_index_prefilter_miss"
    }));
    assert_eq!(
        debug
            .tree_traversal_summary
            .summary_embedding_candidate_count,
        2
    );
    assert_eq!(
        debug
            .tree_traversal_summary
            .summary_embedding_selected_count,
        2
    );
    assert_eq!(
        debug
            .tree_traversal_summary
            .summary_embedding_lookup_batches,
        1
    );
    assert!(!debug.selected_refs.is_empty());
    assert_eq!(debug.injection_ordering.len(), debug.selected_refs.len());
    assert_eq!(debug.injection_ordering[0].prompt_rank, 1);
    assert_eq!(
        debug.injection_ordering[0].ref_hash,
        debug.selected_refs[0].ref_hash
    );
    assert!(debug.injection_ordering[0]
        .selection_reason
        .contains("tree traversal"));
}

// shared-corpus: context_hierarchical_summary_secondary_index_fanout
#[test]
fn context_retrieval_limits_namespace_fanout_with_summary_and_locality_plan() {
    let engine = test_engine();
    let tenant_hash = 606060;
    let mut node_hashes = Vec::new();
    for (index, (title, body)) in [
        (
            "Checkout payment risk",
            "Checkout payment risk score is 91 after the payment gateway timeout.",
        ),
        (
            "Gardening preference",
            "The user likes basil and keeps herbs near the kitchen window.",
        ),
        (
            "Calendar reminder",
            "Team lunch is scheduled next week with no payment discussion.",
        ),
        (
            "Travel note",
            "The train ticket was moved to Friday morning.",
        ),
        (
            "Music note",
            "The playlist now starts with piano practice notes.",
        ),
    ]
    .iter()
    .enumerate()
    {
        let extract = extract_context(
            &engine,
            ContextExtractRequest {
                shard_id: 1,
                tenant_hash,
                source_kind: ContextSourceKind::Chat,
                source_id: format!("fanout-source-{index}"),
                title: (*title).to_string(),
                body: (*body).to_string(),
                timestamp_ms: 10_000 + index as u64,
                provider: ContextModelProviderConfig::default(),
            },
        );
        assert!(extract.status.ok, "{:?}", extract.status);
        node_hashes.push(extract.node.node_hash);
    }

    let report = retrieve_context(
        &engine,
        ContextRetrieveRequest {
            shard_id: 1,
            tenant_hash,
            node_hashes: node_hashes.clone(),
            query: "checkout payment risk score".to_string(),
            start_time_ms: 0,
            end_time_ms: 20_000,
            max_events: 8,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: vec![ContextTier::L0, ContextTier::L1, ContextTier::L2],
            max_summary_nodes: 5,
            max_event_nodes: 1,
            prefer_current_agent: false,
            current_agent_scope_key: "agent:codex".to_string(),
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(report.status.ok, "{:?}", report.status);
    assert_eq!(report.fanout_plan.namespace_node_candidates, 5);
    assert_eq!(report.fanout_plan.summary_candidate_nodes, 5);
    assert_eq!(report.fanout_plan.event_expanded_nodes, 1);
    assert_eq!(report.fanout_plan.skipped_node_count, 4);
    assert!(report.fanout_plan.fanout_reduced);
    // The fanout limit bounds EVENT expansion -- the expensive path -- not summary
    // breadth. All 5 namespace candidates contribute their (cheap) summary, so
    // node_count tracks summary_candidate_nodes; only 1 node is expanded to events and
    // the other 4 are skipped, which is what the assertions above pin down.
    assert_eq!(report.node_count, 5);
    assert_eq!(report.event_count, 1);
    assert!(report
        .fanout_plan
        .locality_keys
        .iter()
        .all(|key| key.starts_with("tenant:606060:node:")));
    assert_eq!(
        report
            .query_understanding_debug
            .tree_traversal_summary
            .namespace_node_candidates,
        5
    );
    assert_eq!(
        report
            .query_understanding_debug
            .tree_traversal_summary
            .event_expanded_node_count,
        1
    );
    assert_eq!(
        report
            .query_understanding_debug
            .tree_traversal_summary
            .skipped_node_count,
        4
    );
    assert!(report
        .blocks
        .iter()
        .any(|block| block.tier == ContextTier::L2));
}

// shared-corpus: context_benchmark_injection_entity_slab_index
#[test]
fn context_benchmark_injection_uses_entity_slab_l0_l1_and_secondary_index() {
    let engine = test_engine();
    let extract = extract_context(
            &engine,
            ContextExtractRequest {
                shard_id: 1,
                tenant_hash: 20260622,
                source_kind: ContextSourceKind::Chat,
                source_id: "locomo-conv-7-session-3-turn-18".to_string(),
                title: "Caroline medical appointment update".to_string(),
                body: "Caroline told Maya on March 12 that her cardiology appointment moved after the museum visit. Maya should remind her brother Leo before Friday. The updated cardiology appointment is now confirmed for the following week.".to_string(),
                timestamp_ms: 1_712_300_000_000,
                provider: ContextModelProviderConfig::default(),
            },
        );
    assert!(extract.status.ok, "{:?}", extract.status);

    let node_entity: crate::ContextNode = extract.node.clone();
    let slab: crate::ContextSlab = extract.event.clone();
    assert_eq!(node_entity.node_hash, extract.index_ref.primary_node_hash);
    assert_eq!(slab.event_id_hash, extract.index_ref.event_id_hash);
    assert!(node_entity.l0.contains("Caroline"));
    assert!(node_entity.l1_ref.contains("kind=Chat"));
    assert!(slab
        .text
        .contains("cardiology appointment moved after the museum visit"));
    assert!(slab.related_node_hashes.is_empty());
    assert_eq!(extract.related_node_hashes, vec![node_entity.node_hash]);


    let retrieve = ContextRetrieveRequest {
        shard_id: 1,
        tenant_hash: 20260622,
        node_hashes: vec![extract.index_ref.primary_node_hash],
        query: "When did Caroline move the cardiology appointment after the museum visit?"
            .to_string(),
        start_time_ms: 1_712_299_000_000,
        end_time_ms: 1_712_301_000_000,
        max_events: 8,
        min_confidence: 0.0,
        min_importance: 0.0,
        tiers: vec![ContextTier::L0, ContextTier::L1, ContextTier::L2],
        max_summary_nodes: 32,
        max_event_nodes: 16,
        prefer_current_agent: false,
        current_agent_scope_key: "agent:codex".to_string(),
        provider: ContextModelProviderConfig::default(),
    };
    let retrieved = retrieve_context(&engine, retrieve.clone());
    assert!(retrieved.status.ok, "{:?}", retrieved.status);
    assert!(retrieved
        .blocks
        .iter()
        .any(|block| { block.tier == ContextTier::L0 && block.text == node_entity.l0 }));
    assert!(retrieved
        .blocks
        .iter()
        .any(|block| block.tier == ContextTier::L1));
    assert!(retrieved
        .blocks
        .iter()
        .any(|block| { block.tier == ContextTier::L2 && block.text == slab.text }));
    let l1_summary = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQuerySummaries {
            tenant_hash: 20260622,
            node_hash: node_entity.node_hash,
            level: 2,
            as_of_ms: slab.event_time_ms,
            limit: Some(2),
        },
    });
    assert!(matches!(
        l1_summary.response,
        CommandResponse::ContextSummaries { ref summaries, .. }
            if summaries.iter().any(|summary| summary.text == node_entity.l1_ref)
    ));

    let inject = inject_context(
        &engine,
        ContextInjectRequest {
            retrieve,
            prompt: "Answer with retrieved TemporalStore memory.".to_string(),
            session_hash: stable_hash64("locomo-conv-7"),
            query_id: "locomo-context-entity-segment-index".to_string(),
            max_prompt_tokens: 192,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(inject.status.ok, "{:?}", inject.status);
    assert!(inject
        .selected_blocks
        .iter()
        .any(|block| block.tier == ContextTier::L0));
    assert!(inject
        .selected_blocks
        .iter()
        .any(|block| block.tier == ContextTier::L1));
    assert!(inject
        .selected_blocks
        .iter()
        .any(|block| block.tier == ContextTier::L2));
    assert!(inject.injected_prompt.contains(&node_entity.l0));
    assert!(inject.injected_prompt.contains("cardiology appointment"));
    assert!(inject.injected_prompt.contains("museum visit"));
    assert!(inject
        .audit
        .selected_refs
        .iter()
        .any(|audit| audit.node_hash == node_entity.node_hash
            && audit.event_time_ms == slab.event_time_ms));
}

// shared-corpus: context_management_ingest_retrieve_pipeline
#[test]
fn context_management_ingest_extract_builds_retrieval_pipeline() {
    let engine = test_engine();
    let manage = context_pipeline_manage_report();
    assert!(manage.pipeline_ready);
    assert!(manage.management_ready);
    assert!(manage.ingestion_extraction_ready);
    assert!(manage.retrieval_ready);
    assert!(manage.injection_ready);
    assert!(manage
        .supported_routes
        .contains(&"/context/ingest_extract".to_string()));
    assert!(manage
        .supported_routes
        .contains(&"/context/manage".to_string()));
    assert_eq!(
        manage.stages,
        vec!["manage", "ingest", "extract", "index", "retrieve", "inject", "audit"]
    );
    assert_eq!(manage.stage_reports.len(), manage.stages.len());
    assert!(manage.stage_reports.iter().all(|stage| stage.ready));
    assert!(manage
        .provider_names
        .contains(&"mock-openai-compatible".to_string()));
    assert!(manage
        .policy_controls
        .contains(&"tenant isolation".to_string()));

    let ingest = ingest_extract_context(
        &engine,
        ContextIngestExtractRequest {
            shard_id: 1,
            tenant_hash: 77,
            sources: vec![
                ContextExtractRequest {
                    shard_id: 999,
                    tenant_hash: 0,
                    source_kind: ContextSourceKind::Incident,
                    source_id: "INC-CTX-1".to_string(),
                    title: "Checkout context incident".to_string(),
                    body: "Checkout retries failed after proxy route movement.".to_string(),
                    timestamp_ms: 1_000,
                    provider: ContextModelProviderConfig::default(),
                },
                ContextExtractRequest {
                    shard_id: 999,
                    tenant_hash: 0,
                    source_kind: ContextSourceKind::Ticket,
                    source_id: "TICKET-CTX-1".to_string(),
                    title: "Support context ticket".to_string(),
                    body: "Support requested retrieval context for the checkout failure."
                        .to_string(),
                    timestamp_ms: 1_500,
                    provider: ContextModelProviderConfig::default(),
                },
            ],
            query: "checkout".to_string(),
            start_time_ms: 0,
            end_time_ms: 3_000,
            max_events: 4,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(ingest.status.ok, "{:?}", ingest.status);
    assert_eq!(ingest.accepted, 2);
    assert_eq!(ingest.failed, 0);
    assert_eq!(ingest.node_hashes.len(), 2);
    assert_eq!(ingest.retrieve_request.shard_id, 1);
    assert_eq!(ingest.retrieve_request.tenant_hash, 77);
    assert_eq!(ingest.retrieve_request.node_hashes, ingest.node_hashes);
    assert_eq!(ingest.summary.source_count, 2);
    assert_eq!(ingest.summary.accepted, 2);
    assert_eq!(ingest.summary.failed, 0);
    assert_eq!(ingest.summary.unique_node_count, 2);
    assert_eq!(ingest.summary.extracted_node_count, 2);
    assert_eq!(ingest.summary.extracted_event_count, 2);
    assert_eq!(ingest.summary.extracted_index_ref_count, 2);
    assert_eq!(ingest.summary.extracted_dirty_marker_count, 2);
    assert_eq!(ingest.summary.extracted_summary_ref_count, 4);
    assert_eq!(ingest.summary.retrieval_node_count, 2);
    assert_eq!(ingest.summary.source_kind_counts.get("incident"), Some(&1));
    assert_eq!(ingest.summary.source_kind_counts.get("ticket"), Some(&1));
    assert_eq!(
        ingest.summary.provider_counts.get("mock-openai-compatible"),
        Some(&2)
    );

    let retrieve = retrieve_context(&engine, ingest.retrieve_request.clone());
    assert!(retrieve.status.ok, "{:?}", retrieve.status);
    assert!(retrieve.event_count >= 2);
    assert!(retrieve
        .blocks
        .iter()
        .any(|block| block.text.to_ascii_lowercase().contains("checkout")));
    assert!(retrieve.parity.pipeline_ready);

    let benchmark = run_context_pipeline_benchmark(
        &engine,
        ContextPipelineBenchmarkRequest {
            shard_id: 1,
            tenant_hash: 88,
            profile: "reference_unit_profile".to_string(),
            source_count: 12,
            query_count: 3,
            max_events: 6,
            provider: ContextModelProviderConfig::default(),
            thresholds: ContextPipelineBenchmarkThresholds::default(),
        },
    );
    assert!(benchmark.status.ok, "{:?}", benchmark.status);
    assert_eq!(
        benchmark.benchmark_name,
        "reference_style_context_management_local"
    );
    assert_eq!(benchmark.profile, "reference_unit_profile");
    assert_ne!(benchmark.workload_signature, 0);
    assert_eq!(benchmark.topic_count, 3);
    assert_eq!(benchmark.min_sources_per_topic, 4);
    assert_eq!(benchmark.max_sources_per_topic, 4);
    assert!(benchmark.source_kind_coverage_count >= 3);
    assert_eq!(benchmark.source_count, 12);
    assert_eq!(benchmark.query_count, 3);
    assert_eq!(benchmark.accepted_sources, 12);
    assert_eq!(benchmark.failed_sources, 0);
    assert_eq!(benchmark.retrieval_successes, 3);
    assert_eq!(benchmark.injection_successes, 3);
    assert_eq!(benchmark.hit_at_k, 1.0);
    assert_eq!(benchmark.mean_reciprocal_rank, 1.0);
    assert_eq!(benchmark.evidence_retention_at_k, 1.0);
    assert!(benchmark.ingest_sources_per_sec > 0.0);
    assert!(benchmark.retrieve_queries_per_sec > 0.0);
    assert!(benchmark.inject_queries_per_sec > 0.0);
    assert_eq!(benchmark.per_query.len(), 3);
    assert!(benchmark
        .per_query
        .iter()
        .all(|query| query.hit_rank.is_some()
            && query.reciprocal_rank > 0.0
            && query.evidence_retained
            && query.expected_topic_source_count == 4));
    assert!(benchmark.recall_at_k >= 1.0);
    assert!(benchmark.token_reduction_percent > 0.0);
    assert!(benchmark.max_selected_tokens_per_query <= 256);
    assert!(benchmark.threshold_passed);
    assert!(benchmark.threshold_violations.is_empty());
    assert_eq!(benchmark.thresholds.min_hit_at_k, 1.0);
    assert_eq!(benchmark.thresholds.min_mean_reciprocal_rank, 0.0);
    assert_eq!(benchmark.thresholds.min_evidence_retention_at_k, 1.0);
    assert_eq!(benchmark.thresholds.max_selected_tokens_per_query, 256);
    assert!(benchmark.source_kind_counts.len() >= 3);
    assert_eq!(
        benchmark.provider_counts.get("mock-openai-compatible"),
        Some(&12)
    );

    let mut unit_sweep_thresholds = ContextPipelineBenchmarkThresholds::default();
    unit_sweep_thresholds.min_ingest_sources_per_sec = 0.0;
    unit_sweep_thresholds.min_retrieve_queries_per_sec = 0.0;
    unit_sweep_thresholds.min_inject_queries_per_sec = 0.0;
    let sweep = run_context_pipeline_benchmark_sweep(
        &engine,
        ContextPipelineBenchmarkSweepRequest {
            shard_id: 1,
            tenant_hash: 100,
            profiles: vec![
                ContextPipelineBenchmarkSweepProfile {
                    profile: "unit_sweep_small".to_string(),
                    source_count: 12,
                    query_count: 2,
                    max_events: 4,
                },
                ContextPipelineBenchmarkSweepProfile {
                    profile: "unit_sweep_medium".to_string(),
                    source_count: 12,
                    query_count: 3,
                    max_events: 6,
                },
            ],
            provider: ContextModelProviderConfig::default(),
            thresholds: unit_sweep_thresholds,
        },
    );
    assert!(sweep.status.ok, "{:?}", sweep.status);
    assert_eq!(
        sweep.benchmark_name,
        "reference_style_context_management_sweep"
    );
    assert_eq!(sweep.profile_count, 2);
    assert!(sweep.all_profiles_ready);
    assert_eq!(sweep.total_sources, 24);
    assert_eq!(sweep.total_queries, 5);
    assert_eq!(sweep.profile_signatures.len(), 2);
    assert!(sweep
        .profile_signatures
        .iter()
        .all(|signature| *signature != 0));
    assert!(sweep.min_sources_per_topic > 0);
    assert!(sweep.max_sources_per_topic >= sweep.min_sources_per_topic);
    assert!(sweep.min_source_kind_coverage_count >= 3);
    assert_eq!(sweep.min_hit_at_k, 1.0);
    assert!(sweep.min_mean_reciprocal_rank > 0.0);
    assert_eq!(sweep.min_evidence_retention_at_k, 1.0);
    assert!(sweep.min_token_reduction_percent > 0.0);
    assert!(sweep.max_selected_tokens_per_query <= 256);
    assert!(sweep.all_thresholds_passed);
