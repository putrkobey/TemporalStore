// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 1, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

// shared-corpus: raft_temporal_raft_process_rollout_evidence
#[test]
fn matrixraft_parity_contract_is_library_consumable_and_openraft_free() {
    let contract = matrixraft_parity_contract();
    assert!(contract.openraft_dependency_removed);
    assert_eq!(
        contract.consensus_backend_boundary,
        "temporalstore_rust::raft::DataRaftConsensusBackend"
    );
    assert!(contract
        .requirements
        .iter()
        .any(|requirement| requirement.id == "leader_write_authority"));
    assert!(contract
        .requirements
        .iter()
        .all(|requirement| requirement.required_for_production));

    let cargo_toml = include_str!("../../../Cargo.toml").to_ascii_lowercase();
    assert!(!cargo_toml.contains("openraft"));
}

// shared-corpus: raft_temporal_raft_process_rollout_evidence raft_temporal_raft_process_read_safety_and_membership_matrix
#[test]
fn matrixraft_parity_report_tracks_distributed_readiness_fields() {
    let readiness = distributed_raft_readiness();
    let report = matrixraft_parity_report(&readiness);
    assert!(report.contract.openraft_dependency_removed);
    assert!(report
        .satisfied
        .contains(&"leader_write_authority".to_string()));
    assert!(report
        .satisfied
        .contains(&"snapshot_tail_catchup".to_string()));
    assert!(report
        .satisfied
        .contains(&"metaserver_membership_workflow".to_string()));
    assert!(report.missing.is_empty(), "missing: {:?}", report.missing);
    assert!(report.ready);
}

// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle
#[test]
fn data_raft_log_codec_round_trips_native_style_header() {
    let entry = DataRaftLogCodecEntry {
        shard_id: 7,
        raft_index: 11,
        log_id: 13,
        log_size: 0,
        wal_sequence: 17,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    };

    let bytes = serialize_data_raft_log(&entry).unwrap();
    assert_eq!(
        u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
        DATA_RAFT_LOG_MAGIC
    );
    assert_eq!(
        u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
        DATA_RAFT_CODEC_VERSION
    );
    let decoded = parse_data_raft_log(&bytes).unwrap();
    assert_eq!(decoded.shard_id, entry.shard_id);
    assert_eq!(decoded.raft_index, entry.raft_index);
    assert_eq!(decoded.log_id, entry.log_id);
    assert_eq!(decoded.wal_sequence, entry.wal_sequence);
    assert_eq!(decoded.command, entry.command);
    assert!(decoded.log_size > 0);
}

// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle
#[test]
fn data_raft_log_codec_rejects_bad_header_and_sequence() {
    let entry = DataRaftLogCodecEntry {
        shard_id: 7,
        raft_index: 11,
        log_id: 13,
        log_size: 0,
        wal_sequence: 17,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    };
    let mut bytes = serialize_data_raft_log(&entry).unwrap();
    bytes[0] = 0;
    assert!(matches!(
        parse_data_raft_log(&bytes),
        Err(RaftError::InvalidDataRaftLog(_))
    ));

    let mut bytes = serialize_data_raft_log(&entry).unwrap();
    bytes[4..8].copy_from_slice(&2_u32.to_le_bytes());
    assert!(matches!(
        parse_data_raft_log(&bytes),
        Err(RaftError::InvalidDataRaftLog(_))
    ));

    let mut bytes = serialize_data_raft_log(&entry).unwrap();
    bytes.truncate(DATA_RAFT_LOG_HEADER_LEN + 1);
    assert!(matches!(
        parse_data_raft_log(&bytes),
        Err(RaftError::InvalidDataRaftLog(_))
    ));

    let zero_sequence = DataRaftLogCodecEntry {
        wal_sequence: 0,
        ..entry
    };
    assert!(matches!(
        serialize_data_raft_log(&zero_sequence),
        Err(RaftError::InvalidDataRaftLog(_))
    ));
}

// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle
#[test]
fn native_data_raft_replication_rejects_corrupt_log_payload() {
    assert!(matches!(
        parse_data_raft_log(b"bad"),
        Err(RaftError::InvalidDataRaftLog(_))
    ));

    let entry = DataRaftLogCodecEntry {
        shard_id: 1,
        raft_index: 1,
        log_id: 1,
        log_size: 0,
        wal_sequence: 1,
        command: Command::StringSet {
            key: "clicks".to_string(),
            value: b"1".to_vec(),
        },
    };
    let mut encoded = serialize_data_raft_log(&entry).unwrap();
    encoded[0] = 0;
    assert!(matches!(
        parse_data_raft_log(&encoded),
        Err(RaftError::InvalidDataRaftLog(_))
    ));
}

// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle
#[test]
fn data_raft_command_codec_round_trips_batch_request() {
    let entry = DataRaftCommandCodecEntry {
        shard_id: 7,
        raft_index: 19,
        request_id: 23,
        commands: vec![
            Command::StringSet {
                key: "k1".to_string(),
                value: b"v1".to_vec(),
            },
            Command::StringSet {
                key: "k2".to_string(),
                value: b"v2".to_vec(),
            },
        ],
    };

    let bytes = serialize_data_raft_command(&entry).unwrap();
    assert_eq!(
        u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
        DATA_RAFT_COMMAND_MAGIC
    );
    let decoded = parse_data_raft_command(&bytes).unwrap();
    assert_eq!(decoded, entry);
}

// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle
#[test]
fn data_raft_command_codec_round_trips_chunked_timestamped_kv_payload() {
    let points = large_feature_points();
    let entry = DataRaftCommandCodecEntry {
        shard_id: 1,
        raft_index: 11,
        request_id: 42,
        commands: vec![Command::FeatureAppend {
            key: "codec-chunked-feature".to_string(),
            points: points.clone(),
        }],
    };

    let bytes = serialize_data_raft_command(&entry).unwrap();
    let decoded = parse_data_raft_command(&bytes).unwrap();
    assert_eq!(decoded, entry);
    assert!(matches!(
        decoded.commands.first(),
        Some(Command::FeatureAppend { key, points: decoded_points })
            if key == "codec-chunked-feature" && decoded_points == &points
    ));
}

// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle
#[test]
fn native_data_raft_replication_rejects_invalid_command_payload() {
    assert!(matches!(
        parse_data_raft_command(b"bad"),
        Err(RaftError::InvalidDataRaftCommand(_))
    ));

    let empty_batch = DataRaftCommandCodecEntry {
        shard_id: 1,
        raft_index: 1,
        request_id: 1,
        commands: Vec::new(),
    };
    assert!(matches!(
        serialize_data_raft_command(&empty_batch),
        Err(RaftError::InvalidDataRaftCommand(_))
    ));

    let missing_partition = DataRaftCommandCodecEntry {
        shard_id: 0,
        raft_index: 1,
        request_id: 1,
        commands: vec![Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        }],
    };
    assert!(matches!(
        serialize_data_raft_command(&missing_partition),
        Err(RaftError::InvalidDataRaftCommand(_))
    ));

    let valid = DataRaftCommandCodecEntry {
        shard_id: 1,
        raft_index: 1,
        request_id: 1,
        commands: vec![Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        }],
    };
    let mut encoded = serialize_data_raft_command(&valid).unwrap();
    encoded[0] = 0;
    assert!(matches!(
        parse_data_raft_command(&encoded),
        Err(RaftError::InvalidDataRaftCommand(_))
    ));
}

#[test]
fn native_data_raft_unavailable_consensus_fails_closed_for_safety_operations() {
    let options = DataRaftConsensusOptions {
        shard_id: 11,
        replica_id: 11,
        group_id: 11,
        ..DataRaftConsensusOptions::default()
    };
    let mut backend = UnavailableDataRaftConsensusBackend::new(options);
    let peer = DataRaftPeer {
        replica_id: 12,
        raft_addr: "127.0.0.1:17012".to_string(),
        snapshot_addr: "127.0.0.1:18012".to_string(),
        auto_promote: false,
    };

    assert!(matches!(backend.start(), Err(RaftError::Transport(_))));
    assert!(!backend.is_leader());
    assert!(matches!(
        backend.propose(b"x".to_vec()),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.wait_for_applied_index(1, 1),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.trigger_snapshot(),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.read_index(1),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.add_peer(peer.clone()),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.add_learner(peer.clone()),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.promote_peer(peer.replica_id),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.remove_peer(peer.replica_id),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.transfer_leader(peer.replica_id),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.campaign(1, false),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.can_serve_bounded_stale_read(0),
        Err(RaftError::Transport(_))
    ));
}

#[cfg(feature = "temporal-raft-engine")]
// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle raft_temporal_raft_process_rollout_evidence
#[test]
fn temporal_raft_data_node_backend_persists_log_snapshot_read_index_and_leader_transfer() {
    use super::temporal_raft_integration::TemporalRaftConsensusBackend;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::default();
    engine.load_shard(7);
    let options = DataRaftConsensusOptions {
        shard_id: 7,
        replica_id: 1,
        group_id: 77,
        wal_dir: Some(dir.path().to_path_buf()),
        peers: vec![
            DataRaftPeer {
                replica_id: 2,
                raft_addr: "127.0.0.1:17002".to_string(),
                snapshot_addr: "127.0.0.1:18002".to_string(),
                auto_promote: false,
            },
            DataRaftPeer {
                replica_id: 3,
                raft_addr: "127.0.0.1:17003".to_string(),
                snapshot_addr: "127.0.0.1:18003".to_string(),
                auto_promote: false,
            },
        ],
        ..DataRaftConsensusOptions::default()
    };
    let mut backend = TemporalRaftConsensusBackend::new_data_node(options.clone(), engine.clone());
    backend.start().unwrap();
    assert!(backend.is_leader());

    let command = Command::StringSet {
        key: "TemporalRaft-k".to_string(),
        value: b"TemporalRaft-v".to_vec(),
    };
    let encoded = serialize_data_raft_log(&DataRaftLogCodecEntry {
        shard_id: 7,
        raft_index: 1,
        log_id: 1,
        log_size: 0,
        wal_sequence: 1,
        command,
    })
    .unwrap();
    let index = backend.propose(encoded).unwrap();
    assert_eq!(index, 1);
    backend.wait_for_applied_index(index, 10).unwrap();
    backend.read_index(10).unwrap();
    let report = backend.report();
    assert!(report.storage_apply_fence_valid);
    assert_eq!(report.storage_apply_fence.shard_id, 7);
    assert_eq!(report.storage_apply_fence.committed_index, index);
    assert_eq!(report.storage_apply_fence.applied_index, index);
    assert!(report.storage_apply_fence.snapshot_id.is_none());

    let read = engine.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringGet {
            key: "TemporalRaft-k".to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"TemporalRaft-v".to_vec())
        }
    );

    let snapshot_index = backend.trigger_snapshot().unwrap();
    assert_eq!(snapshot_index, index);
    let meta = backend.build_temporal_raft_snapshot_meta_compat();
    assert_eq!(meta.last_log_id.unwrap().index, index);
    let report = backend.report();
    assert!(report.snapshot_installed);
    assert!(report.storage_apply_fence_valid);
    assert_eq!(
        report.storage_apply_fence.snapshot_id.as_deref(),
        Some(meta.snapshot_id.as_str())
    );
    assert_eq!(report.storage_apply_fence.storage_epoch, index);

    backend.transfer_leader(2).unwrap();
    assert!(!backend.is_leader());
    backend.campaign(10, false).unwrap();
    assert!(backend.is_leader());
    let restored = TemporalRaftConsensusBackend::new_data_node(options, engine);
    let status = restored.status().unwrap();
    assert_eq!(status.leader_replica_id, 1);
    assert_eq!(status.applied_index, 1);
    assert_eq!(status.first_index, 2);
    assert_eq!(status.fatal_event_count, 0);
    assert!(!status.snapshot_creating);
    assert!(!status.snapshot_loading);
    assert_eq!(restored.report().durable_log_records, 0);
    assert!(restored.report().storage_apply_fence_valid);
    assert_eq!(restored.report().storage_apply_fence.applied_index, 1);
    assert!(restored.report().storage_apply_fence.snapshot_id.is_some());
    assert!(restored.report().campaign_supported);
    assert!(restored.report().learner_bootstrap_supported);
}

#[cfg(feature = "temporal-raft-engine")]
#[test]
#[should_panic(expected = "temporal raft storage fence checksum mismatch")]
fn temporal_raft_data_node_backend_rejects_corrupt_storage_apply_fence_on_restart() {
    use super::temporal_raft_integration::TemporalRaftConsensusBackend;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::default();
    engine.load_shard(7);
    let options = DataRaftConsensusOptions {
        shard_id: 7,
        replica_id: 1,
        group_id: 77,
        wal_dir: Some(dir.path().to_path_buf()),
        ..DataRaftConsensusOptions::default()
    };
    let mut backend = TemporalRaftConsensusBackend::new_data_node(options.clone(), engine.clone());
    backend.start().unwrap();
    let encoded = serialize_data_raft_log(&DataRaftLogCodecEntry {
        shard_id: 7,
        raft_index: 1,
        log_id: 1,
        log_size: 0,
        wal_sequence: 1,
        command: Command::StringSet {
            key: "TemporalRaft-corrupt-fence".to_string(),
            value: b"value".to_vec(),
        },
    })
    .unwrap();
    backend.propose(encoded).unwrap();
    backend.trigger_snapshot().unwrap();

    let path = dir.path().join("temporalraft-7-1.json");
    let bytes = std::fs::read(&path).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["storage_apply_fence"]["checksum"] = serde_json::Value::String("corrupt".to_string());
    std::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();

    let _ = TemporalRaftConsensusBackend::new_data_node(options, engine);
}

#[cfg(feature = "temporal-raft-engine")]
// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix
#[test]
fn temporal_raft_data_node_backend_bootstraps_learner_and_auto_promotes_peer() {
    use super::temporal_raft_integration::TemporalRaftConsensusBackend;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::default();
    let options = DataRaftConsensusOptions {
        shard_id: 8,
        replica_id: 4,
        group_id: 78,
        wal_dir: Some(dir.path().to_path_buf()),
        bootstrap_as_learner: true,
        peers: vec![
            DataRaftPeer {
                replica_id: 1,
                raft_addr: "127.0.0.1:17001".to_string(),
                snapshot_addr: "127.0.0.1:18001".to_string(),
                auto_promote: false,
            },
            DataRaftPeer {
                replica_id: 2,
                raft_addr: "127.0.0.1:17002".to_string(),
                snapshot_addr: "127.0.0.1:18002".to_string(),
                auto_promote: false,
            },
        ],
        ..DataRaftConsensusOptions::default()
    };
    let mut backend = TemporalRaftConsensusBackend::new_data_node(options.clone(), engine);
    backend.start().unwrap();

    let status = backend.status().unwrap();
    assert!(status.learner);
    assert!(!status.leader);
    assert_eq!(status.voter_count, 2);
    assert_eq!(status.learner_count, 1);
    assert!(matches!(
        backend.campaign(10, false),
        Err(RaftError::NodeNotFound(4))
    ));

    backend
        .add_learner(DataRaftPeer {
            replica_id: 5,
            raft_addr: "127.0.0.1:17005".to_string(),
            snapshot_addr: "127.0.0.1:18005".to_string(),
            auto_promote: true,
        })
        .unwrap();
    let status = backend.status().unwrap();
    assert_eq!(status.voter_count, 3);
    assert_eq!(status.learner_count, 1);

    let restored = TemporalRaftConsensusBackend::new_data_node(options, TemporalEngine::default());
    let restored_status = restored.status().unwrap();
    assert!(restored_status.learner);
    assert_eq!(restored_status.voter_count, 3);
    assert_eq!(restored_status.learner_count, 1);
}

