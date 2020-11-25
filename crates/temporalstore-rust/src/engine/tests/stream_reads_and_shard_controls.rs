// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Part 3 of engine tests, split from engine/engine_regression_tests.rs.
#![allow(clippy::all)]
use super::*;

#[test]
fn control_api_reads_block_and_index_streams() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"stream-value".to_vec(),
        },
    });

    let block = engine.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Block,
        block_slab_id: 0,
        offset: 0,
        size: 12,
    });
    assert_eq!(block.data, b"stream-value".to_vec());

    let index = engine.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Index,
        block_slab_id: 0,
        offset: 0,
        size: 32,
    });
    assert!(index.status.ok);
    assert!(!index.data.is_empty());

    let scan = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Block,
        block_slab_id: 0,
        start_offset: 0,
        end_offset: 12,
        max_bytes: 12,
    });
    assert_eq!(scan.records.len(), 1);
    assert_eq!(scan.records[0].data, b"stream-value".to_vec());

    let invalid = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Block,
        block_slab_id: 0,
        start_offset: 12,
        end_offset: 1,
        max_bytes: 12,
    });
    assert_eq!(invalid.status.code, "invalid_stream_range");
    assert!(invalid.records.is_empty());
}

#[test]
fn a_scan_cut_short_by_its_budget_does_not_claim_the_stream_ended() {
    // scan_stream stops walking for two unrelated reasons -- the window ended, or max_bytes ran
    // out -- and it answered end_of_stream: true for both. So a caller reading a range larger
    // than its budget was handed a prefix and told it had the whole thing, with nothing in the
    // response to suggest otherwise.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for i in 0..8 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{i}"),
                value: vec![b'v'; 64],
            },
        });
    }

    let all = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Wal,
        block_slab_id: 0,
        start_offset: 0,
        end_offset: u64::MAX,
        max_bytes: u64::MAX,
    });
    assert!(all.status.ok);
    assert!(
        all.records.len() >= 4,
        "premise: there are several records to walk, got {}",
        all.records.len()
    );
    assert!(
        all.end_of_stream,
        "a scan with no budget to run out of did reach the end of the window"
    );

    // A budget that fits the first record and not the second.
    let budget = all.records[0].data.len() as u64 + 1;
    let cut = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Wal,
        block_slab_id: 0,
        start_offset: 0,
        end_offset: u64::MAX,
        max_bytes: budget,
    });
    assert!(cut.status.ok);
    assert!(
        cut.records.len() < all.records.len(),
        "premise: the budget cut this scan short, got {} of {}",
        cut.records.len(),
        all.records.len()
    );
    assert!(
        !cut.end_of_stream,
        "the budget stopped this scan with records still in the window, so it must not report          the stream as ended"
    );
}

#[test]
fn control_api_reads_and_scans_wal_stream() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k2".to_string(),
            value: b"v2".to_vec(),
        },
    });

    let stream = engine.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Wal,
        block_slab_id: 0,
        offset: 0,
        size: 4096,
    });
    assert!(stream.status.ok);
    // Decode the records rather than matching on how a field is spelled: the endpoint returns
    // the log's records, and that is what should be asserted.
    // Walk the frames the stream carries rather than splitting it on newlines: a record ends
    // where its frame says it does, and a length-framed payload contains 0x0A of its own.
    let mut sequences: Vec<u64> = Vec::new();
    let mut at = 0usize;
    while at < stream.data.len() {
        if stream.data[at] == 0 {
            break;
        }
        match crate::log_framing::next_frame(&stream.data[at..]) {
            Ok(Some((consumed, _))) if consumed > 0 => {
                sequences.push(
                    crate::wal::decode_wal_line(&stream.data[at..at + consumed])
                        .expect("the stream should carry decodable records")
                        .sequence,
                );
                at += consumed;
            }
            _ => break,
        }
    }
    assert_eq!(
        sequences,
        vec![1, 2],
        "the stream should carry both records, in order"
    );

    let scan = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Wal,
        block_slab_id: 0,
        start_offset: 0,
        end_offset: 4096,
        max_bytes: 4096,
    });
    assert_eq!(scan.records.len(), 2);
    assert_eq!(
        engine
            .get_stats(1)
            .stats
            .unwrap()
            .write_ahead_log
            .last_sequence,
        2
    );
}

#[test]
fn control_api_reads_and_scans_index_log_stream() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"hv".to_vec(),
        },
    });

    let stream = engine.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::IndexLog,
        block_slab_id: 0,
        offset: 0,
        size: 8192,
    });
    assert!(stream.status.ok);
    // The index-log advances one record per write. Reading the stream as TEXT and matching a
    // field's spelling was an assumption about the encoding, not about the advance this test
    // is for -- and a record's payload need not be text at all. Walk it the way a reader does
    // and assert over the sequences themselves.
    let mut sequences = Vec::new();
    let mut at = 0usize;
    while at < stream.data.len() {
        let Some((consumed, payload)) = crate::log_framing::next_frame(&stream.data[at..]).unwrap()
        else {
            break;
        };
        let record: crate::index_log::IndexDeltaRecord =
            crate::index_log::decode_index_payload(payload).unwrap();
        sequences.push(record.sequence);
        at += consumed;
    }
    assert_eq!(sequences, vec![1, 2], "one record per write, in order");

    let scan = engine.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::IndexLog,
        block_slab_id: 0,
        start_offset: 0,
        end_offset: 8192,
        max_bytes: 8192,
    });
    assert_eq!(scan.records.len(), 2);
    assert_eq!(engine.index_log_store().stats(1).last_sequence, 2);

    // Content is asserted over the folded served-index reconstruction (export_index_bytes),
    // which is the complete, current ShardState in both the whole-index (base-file) and the
    // delta (live-in-memory) paths -- the index-log itself is a per-write log, not the
    // authoritative complete image.
    // The served index is written in whichever format the container gate selects, so decode it
    // through the funnel a reader uses and assert over the STATE. Reading the bytes as JSON was an
    // assumption about the encoding, not about the thing being tested.
    let served: serde_json::Value = serde_json::to_value(
        crate::engine::decode_index_bytes(&engine.export_index_bytes(1).unwrap())
            .expect("served index decodes"),
    )
    .expect("shard state re-serializes for assertion");
    // `hashes` IS SERIALIZED NOW, AND THIS COMMENT SAID THE OPPOSITE. It read "`hashes` is
    // deliberately NOT serialized (`skip_serializing`): it is rebuildable from the durable bucket
    // index on load, and duplicating those page references in every checkpoint is what made large
    // context backfills tens of MB heavier." It carries `#[serde(default)]` -- the element name
    // came off the page entry, so nothing can rebuild that map from the index and it is the
    // durable authority. What is asserted below is unchanged: that the hash write is recorded in
    // the bucket index with the right page address.
    //
    // The bucket-index page entries serialize under abbreviated field names on some builds and
    // full names on others, so accept either: what is being asserted is that the hash write is
    // recorded with the right page address, not how the fields are spelled.
    //
    // AND THE SELECTOR CANNOT NAME THE FIELD ANY MORE. It matched
    // `object_key == "h" && component == "f"`, and a page entry has no component slot in the
    // served index, so it matched nothing and read "the hash h/f write is recorded in the bucket
    // index" for a write that was recorded. The object holds exactly ONE field, so its page is
    // selected by object key alone -- and the count below is what keeps that selection exact
    // rather than letting it widen to any page of any object.
    let hash_blocks: Vec<&serde_json::Value> = served["slot_index"]["bucket_map"]
        .as_object()
        .expect("served index carries the bucket map")
        .values()
        .filter_map(|bucket| {
            bucket
                .get("page_index")
                .or_else(|| bucket.get("p"))
                .and_then(|blocks| blocks.as_object())
        })
        .flat_map(|blocks| blocks.values())
        .filter(|block| {
            let key = block.get("object_key").or_else(|| block.get("k"));
            key == Some(&serde_json::json!("h"))
        })
        .collect();
    assert_eq!(
        1,
        hash_blocks.len(),
        "the served index records {} page entries for the one-field hash `h`, not one",
        hash_blocks.len()
    );
    let hash_block = hash_blocks[0];
    // AND NO ELEMENT-NAME SLOT IS WRITTEN, which is the other half of why the selector changed. A
    // `component` or `c` key on a page entry here would mean the element name has come back to the
    // STORED form, which is the thing the width step removed.
    assert!(
        hash_block.get("component").is_none() && hash_block.get("c").is_none(),
        "the served index still writes an element name on a page entry: {hash_block}"
    );
    let address = hash_block
        .get("address")
        .or_else(|| hash_block.get("a"))
        .expect("the recorded page carries its address");
    // ONE WORD, NOT TWO FIELDS. The slab id and the offset are the two halves of `a`, so the
    // served index carries the word and the slab is the high 32 bits of it -- there is no `ps`
    // key to read any more, and this asserts that there is not, so a reader that went back to
    // looking for one would fail here rather than quietly finding nothing.
    let word = address
        .get("a")
        .and_then(serde_json::Value::as_u64)
        .expect("the address carries its packed address word");
    assert!(
        address.get("ps").is_none() && address.get("o").is_none(),
        "the served index still writes the split slab id and offset: {address}"
    );
    assert_eq!(
        crate::block_store::extract_block_slab_id(word),
        0,
        "the high half of the address word is the slab id"
    );
    // THE STRING ROW, UNDER THE CONSOLIDATED MAP'S OWN SPELLING.
    //
    // This read `served["strings"]`, and there has been no `strings` field on a `ShardState` since
    // the string map became one KIND of the consolidated `objects` map, keyed kind then object
    // key. So the index answered a JSON null, `get("k1")` answered `None`, and the assertion failed
    // for a row that is present and correct. The same test already reads `served["slot_index"]`
    // above for exactly this reason; this spelling was the one left behind.
    //
    // THE KIND'S SPELLING IS ASKED FOR RATHER THAN WRITTEN OUT. `StoredModelKind` IS what
    // serializes as the outer key -- through its own `Serialize` impl, which writes `as_str()` --
    // so taking it from there cannot go stale the way a literal just did.
    let string_kind = crate::engine::storage_bucket_internals::StoredModelKind::String.as_str();
    assert!(
        served["objects"][string_kind].get("k1").is_some(),
        "the served index carries no string row for `k1` under `objects.{string_kind}`; the \
         consolidated map holds {}",
        served["objects"],
    );
}

#[test]
fn readonly_shard_rejects_writes_but_allows_reads() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                load_version: 1,
                local_node_id: None,
                shard_uri: "file:///tmp/readonly".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 99,
                readonly: true,
                table_name: "table".to_string(),
            })
            .status
            .ok
    );

    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(!write.status.ok);
    assert_eq!(write.status.code, "readonly_shard");

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert!(read.status.ok);
    assert_eq!(read.response, CommandResponse::Bytes { value: None });
}

#[test]
fn checked_execute_rejects_stale_load_version() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                load_version: 7,
                local_node_id: None,
                shard_uri: "file:///tmp/versioned".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 99,
                readonly: false,
                table_name: "table".to_string(),
            })
            .status
            .ok
    );

    let stale = engine.execute_checked(CheckedExecuteRequest {
        shard_id: 1,
        load_version: 6,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert_eq!(stale.status.code, "load_version_mismatch");

    let current = engine.execute_checked(CheckedExecuteRequest {
        shard_id: 1,
        load_version: 7,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(current.status.ok);
}

#[test]
fn loaded_shard_stats_reports_per_shard_load() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.load_shard(2);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "a".to_string(),
            value: b"1".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"2".to_vec(),
        },
    });

    let stats = engine.loaded_shard_stats();
    assert_eq!(stats.len(), 2);
    assert!(stats
        .iter()
        .any(|stat| stat.shard_id == 1 && stat.string_records == 1));
    assert!(stats
        .iter()
        .any(|stat| stat.shard_id == 2 && stat.hash_records == 1));
}

#[test]
fn string_set_conditional_supports_nx_xx_and_get() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetConditional {
            key: "k".to_string(),
            value: b"v1".to_vec(),
            ttl_ms: None,
            condition: StringSetCondition::IfNotExists,
            return_old: false,
            keep_ttl: false,
        },
    });
    assert_eq!(first.response, CommandResponse::Integer { value: 1 });

    let rejected = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetConditional {
            key: "k".to_string(),
            value: b"v2".to_vec(),
            ttl_ms: None,
            condition: StringSetCondition::IfNotExists,
            return_old: false,
            keep_ttl: false,
        },
    });
    assert_eq!(rejected.response, CommandResponse::Integer { value: 0 });

    let old = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetConditional {
            key: "k".to_string(),
            value: b"v3".to_vec(),
            ttl_ms: None,
            condition: StringSetCondition::IfExists,
            return_old: true,
            keep_ttl: false,
        },
    });
    assert_eq!(
        old.response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k".to_string()
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v3".to_vec())
        }
    );
}

#[test]
fn recovery_validates_all_timestamped_kv_block_families() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let feature_points = (0..8)
        .map(|idx| FeaturePoint {
            timestamp_ms: 1_000 + idx,
            value: vec![b'f'; 10 * 1024],
        })
        .collect::<Vec<_>>();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: "all-family-feature".to_string(),
                    points: feature_points.clone(),
                },
            })
            .status
            .ok
    );

    let sequence_rows = (0..8)
        .map(|idx| SequenceFeatureRow {
            timestamp_ms: 2_000 + idx,
            gid: idx,
            action_type: 7,
            duration: 11,
            author_id: 13,
        })
        .collect::<Vec<_>>();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceAdd {
                    key: "all-family-sequence".to_string(),
                    rows: sequence_rows.clone(),
                },
            })
            .status
            .ok
    );

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextWriteEvent {
                    tenant_hash: 44,
                    node_hash: 55,
                    event: Box::new(ContextEvent {
                        event_id_hash: 66,
                        event_time_ms: 4_000,
                        ingestion_time_ms: 4_000,
                        kind: 1,
                        event_type: 2,
                        actor_hash: 77,
                        status: 1,
                        valid_until_ms: 0,
                        confidence: 0.99,
                        importance: 0.75,
                        text: "context event".to_string(),
                        source_ref: "local://test".to_string(),
                        related_node_hashes: vec![55],
                        compact_attrs: vec![1, 2, 3],
                        vector: Vec::new(),
                    }),
                    first_write_only: false,
                    cold_storage: false,
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextWriteIndexRef {
                    tenant_hash: 44,
                    index_name: "actor".to_string(),
                    index_value_hash: 77,
                    scope_hash: 1,
                    event_time_ms: 4_000,
                    index_ref: ContextIndexRef {
                        primary_node_hash: 55,
                        primary_event_time_ms: 4_000,
                        event_id_hash: 66,
                    },
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextWritePackAudit {
                    tenant_hash: 44,
                    audit: ContextPackAudit {
                        query_id: "q-all-family".to_string(),
                        session_hash: 88,
                        request_time_ms: 4_100,
                        query_hash: 99,
                        max_prompt_tokens: 128,
                        selected_tokens: 32,
                        selected_refs: vec![ContextAuditRef {
                            node_hash: 55,
                            event_time_ms: 4_000,
                            reason: "selected".to_string(),
                        }],
                        blocked_refs: Vec::new(),
                    },
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextMarkSummaryDirty {
                    tenant_hash: 44,
                    node_hash: 55,
                event_time_ms: 4_200,
                reason: 9,
                propagate_depth: 2,
                },
            })
            .status
            .ok
    );

    let report = engine.storage_recovery_report(1);
    // context_dirty tracking was intentionally moved to an ephemeral in-memory
    // coalesced index (commit 9390d110), so it no longer contributes a persisted
    // timestamped page: feature 16 (8 feature + 8 sequence, now folded into the
    // feature family) + context_event/index/audit 1 each = 19.
    assert_eq!(report.feature_block_layout.indexed_timestamped_points, 19);
    // Every page the index points at is a packed page, which is the property this is for and
    // does not depend on how densely one packs. The count this replaced -- at least ten pages --
    // moved when the page format did: the same 19 points, asserted just above, now occupy 6 pages
    // because a point costs about a quarter of what it cost as JSON. Ten LEGACY pages would have
    // satisfied the old assertion; none satisfy this one.
    assert_eq!(
        report.feature_block_layout.packed_timestamped_blocks,
        report.feature_block_layout.unique_timestamped_block_refs,
        "pages landed as: {} packed, {} legacy, {} corrupt, {} unique refs; first corrupt: {:?}",
        report.feature_block_layout.packed_timestamped_blocks,
        report.feature_block_layout.legacy_timestamped_value_blocks,
        report.feature_block_layout.corrupt_packed_feature_blocks.len(),
        report.feature_block_layout.unique_timestamped_block_refs,
        report.feature_block_layout.corrupt_packed_feature_blocks.first()
    );
    assert!(
        report.feature_block_layout.packed_timestamped_blocks > 0,
        "no packed pages at all, so the assertion above proved nothing"
    );
    assert_eq!(
        report.feature_block_layout.legacy_timestamped_value_blocks, 0,
        "a legacy single-value page survived where a packed one was expected"
    );
    assert!(
        report
            .feature_block_layout
            .unique_timestamped_block_refs
            .saturating_sub(report.feature_block_layout.packed_timestamped_blocks)
            <= report.feature_block_layout.legacy_timestamped_value_blocks
    );
    assert!(report
        .feature_block_layout
        .corrupt_packed_feature_blocks
        .is_empty());
    assert!(report
        .feature_block_layout
        .missing_indexed_timestamps
        .is_empty());
    assert!(report
        .feature_block_layout
        .orphan_packed_timestamps
        .is_empty());
    assert!(report
        .feature_block_layout
        .duplicate_packed_timestamps
        .is_empty());

    let families = report
        .feature_block_layout
        .families
        .iter()
        .map(|family| (family.kind.as_str(), family))
        .collect::<BTreeMap<_, _>>();
    // Sequence folds into the feature family (same timestamped-KV storage, typed row
    // codec at the API layer), so there is no distinct "sequence" recovery family.
    for kind in [
        "feature",
        "context_event",
        "context_index",
        "context_audit",
    ] {
        let family = families.get(kind).expect("timestamped family report");
        assert!(family.indexed_points > 0, "{kind}");
        assert!(family.packed_blocks > 0, "{kind}");
        assert_eq!(family.corrupt_blocks, 0, "{kind}");
        assert_eq!(family.mismatch_count, 0, "{kind}");
    }
    assert!(
        families
            .get("feature")
            .expect("feature family")
            .unique_block_refs
            > 1
    );

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureQuery {
                    key: "all-family-feature".to_string(),
                    start_ms: 1_000,
                    end_ms: 1_010,
                    count: None,
                },
            })
            .response,
        CommandResponse::FeaturePoints {
            points: feature_points
        }
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceQuery {
                    key: "all-family-sequence".to_string(),
                    start_ms: 2_000,
                    end_ms: 2_010,
                    count: 16,
                    filters: Vec::new(),
                },
            })
            .response,
        CommandResponse::SequenceRows {
            rows: sequence_rows
        }
    );
}

#[test]
fn control_state_change_matches_distinct_field_semantics() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (timestamp_ms, value) in [(10, "device-a"), (20, "device-a"), (30, "device-b")] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateChangeAdd {
                key: "control_state-change".to_string(),
                timestamp_ms,
                value: value.as_bytes().to_vec(),
                precision_ms: Some(10),
                ttl_ms: None,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateQuery {
                    key: "control_state-change".to_string(),
                    start_ms: 0,
                    end_ms: 40,
                    aggregator: "change".to_string(),
                },
            })
            .response,
        CommandResponse::Integer { value: 2 }
    );

    for (timestamp_ms, value) in [(10, "buyer-1"), (20, "buyer-1"), (30, "buyer-2")] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateChangeAdd {
