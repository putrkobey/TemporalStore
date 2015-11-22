// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 5: incremental raft WAL records, and the networked-election primitives
//! the production timer loop drives.
#![allow(clippy::all)]
use super::helpers::*;
use super::*;

fn wal_dir_bytes(root: &std::path::Path) -> u64 {
    fn walk(path: &std::path::Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                walk(&entry.path(), total);
            } else {
                *total += meta.len();
            }
        }
    }
    let mut total = 0;
    walk(root, &mut total);
    total
}

fn propose_range(cluster: &RaftCluster, from: usize, to: usize) {
    for i in from..to {
        cluster
            .propose(Command::StringSet {
                key: format!("wal-cost-{i}"),
                value: vec![7u8; 32],
            })
            .unwrap();
    }
}

/// The WAL record Raft hands to `persist_configured_wal` carries the node's whole log.
/// Writing that verbatim on every append makes each append cost O(log length), which is
/// what made write latency climb with the log. Appends must instead cost about the same
/// no matter how long the log already is.
#[test]
fn wal_append_cost_stays_flat_as_the_log_grows() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 91, [1], RaftConfig::default()).unwrap();

    propose_range(&cluster, 0, 50);
    let before_early = wal_dir_bytes(dir.path());
    propose_range(&cluster, 50, 100);
    let early_block = wal_dir_bytes(dir.path()) - before_early;

    propose_range(&cluster, 100, 450);
    let before_late = wal_dir_bytes(dir.path());
    propose_range(&cluster, 450, 500);
    let late_block = wal_dir_bytes(dir.path()) - before_late;

    // With a full-log payload the later block rewrites a log ~6x longer on every append,
    // so it costs ~6x the earlier block. Incremental records keep the two comparable.
    assert!(
        late_block < early_block * 2,
        "50 appends at log length ~450 cost {late_block} bytes vs {early_block} at length ~50; \
         the payload is still growing with the log"
    );
}

/// Folding incremental records back together has to reproduce the log exactly, including
/// across the segment rotations that write a fresh base record.
#[test]
fn incremental_wal_records_recover_the_whole_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = RaftConfig::default();
    // Small segments so the run crosses several rotations.
    config.max_segment_bytes = 4096;
    config.min_keep_segment_num = 64;
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 92, [1], config.clone()).unwrap();
    for i in 0..200 {
        cluster
            .propose(Command::StringSet {
                key: format!("recover-{i}"),
                value: format!("value-{i}").into_bytes(),
            })
            .unwrap();
    }
    let expected = cluster.status().commit_index;
    assert_eq!(expected, 200);

    let restored =
        RaftCluster::restore_single_shard_from_wal(dir.path(), 92, [1], config).unwrap();
    assert_eq!(restored.status().commit_index, expected);
    let response = restored
        .read_local(
            1,
            Command::StringGet {
                key: "recover-199".to_string(),
            },
        )
        .unwrap();
    assert_eq!(
        response,
        CommandResponse::Bytes {
            value: Some(b"value-199".to_vec())
        }
    );
    let wal = LocalRaftWal::new(dir.path());
    let record = wal.load_node(92, 1).unwrap().unwrap();
    assert_eq!(record.entries.len(), 200);
    assert_eq!(record.entries.last().unwrap().index, 200);
}

/// A raft leader change can truncate a divergent suffix. The entry the WAL last wrote is
/// then gone, so an incremental record would fold onto a log that no longer exists --
/// the writer has to notice and re-base instead.
#[test]
fn wal_rebases_when_the_log_suffix_is_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 93, [1], RaftConfig::default()).unwrap();
    for i in 0..20 {
        cluster
            .propose(Command::StringSet {
                key: format!("conflict-{i}"),
                value: vec![1u8; 8],
            })
            .unwrap();
    }
    let wal = LocalRaftWal::new(dir.path());
    let mut record = wal.load_node(93, 1).unwrap().unwrap();
    assert_eq!(record.entries.len(), 20);

    // Simulate a new leader overwriting indexes 11..=15 at a higher term.
    record.entries.truncate(10);
    for index in 11..=15u64 {
        record.entries.push(RaftLogEntry {
            leader_time_ms: 0,
            term: 9,
            index,
            shard_id: 93,
            command: Command::StringSet {
                key: format!("overwritten-{index}"),
                value: vec![2u8; 8],
            },
        });
    }
    record.hard_state.commit_index = 15;
    wal.persist_node_segmented(93, 1, &record, 1024 * 1024, 8)
        .unwrap();

    let recovered = wal.recover_node_segmented(93, 1).unwrap().record.unwrap();
    assert_eq!(recovered.entries.len(), 15);
    assert_eq!(recovered.entries.last().unwrap().index, 15);
    assert_eq!(recovered.entries.last().unwrap().term, 9);
    assert!(recovered
        .entries
        .iter()
        .all(|entry| entry.index <= 10 || entry.term == 9));
}

/// A snapshot compaction drops a prefix of the log. Recovery must drop it too rather
/// than resurrect entries from the base record it folded onto.
#[test]
fn wal_recovery_drops_entries_compacted_away() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 94, [1], RaftConfig::default()).unwrap();
    for i in 0..20 {
        cluster
            .propose(Command::StringSet {
                key: format!("compact-{i}"),
                value: vec![3u8; 8],
            })
            .unwrap();
    }
    let wal = LocalRaftWal::new(dir.path());
    let mut record = wal.load_node(94, 1).unwrap().unwrap();
    // Keep only the tail, as a snapshot install would.
    record.entries.retain(|entry| entry.index > 12);
    wal.persist_node_segmented(94, 1, &record, 1024 * 1024, 8)
        .unwrap();

    let recovered = wal.recover_node_segmented(94, 1).unwrap().record.unwrap();
    assert_eq!(recovered.entries.first().unwrap().index, 13);
    assert_eq!(recovered.entries.len(), 8);
}

/// `prepare_campaign` must make the term bump and self-vote durable before any vote is
/// requested, and `conclude_campaign` must refuse to install a minority leader.
#[test]
fn networked_campaign_promotes_only_with_a_majority() {
    let cluster = RaftCluster::new_single_shard(95, [1, 2, 3]);
    assert_eq!(cluster.leader_id(), 1);
    let before_term = cluster.status().current_term;

    let template = cluster.prepare_campaign(2).unwrap();
    assert_eq!(template.candidate_id, 2);
    assert_eq!(template.shard_id, 95);
    assert!(template.term > before_term);

    // One grant (its own) out of three voters is a minority.
    assert!(!cluster.conclude_campaign(2, template.term, 1).unwrap());
    assert_eq!(
        cluster.leader_id(),
        1,
        "a minority campaign must not install a leader"
    );

    // Two of three is a majority.
    assert!(cluster.conclude_campaign(2, template.term, 2).unwrap());
    assert_eq!(cluster.leader_id(), 2);
    assert_eq!(cluster.status().current_term, template.term);
}

/// A peer answering with a newer term means someone else has already moved on; the
/// candidate has to step down instead of installing itself at a stale term.
#[test]
fn campaign_stands_down_on_a_newer_peer_term() {
    let cluster = RaftCluster::new_single_shard(96, [1, 2, 3]);
    let template = cluster.prepare_campaign(3).unwrap();
    let candidate_term = |cluster: &RaftCluster| {
        cluster
            .status()
            .nodes
            .into_iter()
            .find(|node| node.node_id == 3)
            .map(|node| node.current_term)
            .unwrap()
    };
    assert_eq!(candidate_term(&cluster), template.term);

    cluster.observe_higher_term(3, template.term + 5).unwrap();
    assert_eq!(candidate_term(&cluster), template.term + 5);

    // Votes arrive over the network, so they can land after the candidate has already
    // stepped down. A unanimous tally for the abandoned term must not install it.
    assert!(
        !cluster.conclude_campaign(3, template.term, 3).unwrap(),
        "grants for an abandoned term must not promote"
    );
    assert_eq!(cluster.leader_id(), 1);
}

/// The follower side of failure detection: a leader's AppendEntries is the only signal
/// that it is still there, so accepting one has to be observable.
#[test]
fn leader_contact_epoch_advances_on_append_entries() {
    let cluster = RaftCluster::new_single_shard(97, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "contact".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    let before = cluster.leader_contact_epoch();
    let request = cluster.build_append_entries_request(2).unwrap();
    cluster.receive_append_entries(request).unwrap();
    assert!(
        cluster.leader_contact_epoch() > before,
        "accepting AppendEntries must record leader contact"
    );
}

/// With shadow election off, `tick_election` may age the election clock but must never
/// promote: in a real deployment this process only sees its own copy of peer state, and
/// promoting a remote node here would just make it disagree with the rest of the group.
#[test]
fn shadow_election_off_never_promotes_locally() {
    let guarded = RaftCluster::new_single_shard(98, [1, 2, 3]);
    guarded.set_local_shadow_election(false);
    guarded.set_alive(1, false).unwrap();
    for _ in 0..32 {
        let _ = guarded.tick_election();
    }
    assert_eq!(
        guarded.leader_id(),
        1,
        "a production runtime must decide leadership over the wire, not locally"
    );

    // The in-process model keeps its old behavior.
    let shadow = RaftCluster::new_single_shard(99, [1, 2, 3]);
    shadow.set_alive(1, false).unwrap();
    for _ in 0..32 {
        let _ = shadow.tick_election();
    }
    assert_ne!(shadow.leader_id(), 1);
}

/// A leader promoted by a networked election has never replicated to its peers, so this
/// process knows nothing about how far they got. It must still be able to send the probe
/// that discovers it. When the pipeline instead assumed the whole log was in flight, that
/// probe was rejected with backpressure, the new leader could never commit in its own
/// term, and the surviving replicas duelled for leadership indefinitely.
#[test]
fn promoted_leader_can_reach_peers_it_knows_nothing_about() {
    let cluster = RaftCluster::new_single_shard(100, [1, 2, 3]);
    for i in 0..2000 {
        cluster
            .propose(Command::StringSet {
                key: format!("backpressure-{i}"),
                value: vec![9u8; 16],
            })
            .unwrap();
    }
    // A process that was never leader has no confirmed progress for its peers: it only
    // ever learns that from AppendEntries responses it sent itself.
    {
        let mut inner = cluster.inner.write().unwrap();
        for node in inner.nodes.values_mut() {
            if node.id != 2 {
                node.commit_index = 0;
                node.applied_index = 0;
                node.pipeline_state.match_index = 0;
            }
        }
    }

    let template = cluster.prepare_campaign(2).unwrap();
    assert!(cluster.conclude_campaign(2, template.term, 2).unwrap());
    assert_eq!(cluster.leader_id(), 2);

    // Anything that refreshes the pipeline (an incoming AppendEntries, a propose) runs
    // before the new leader gets its first response back, and must not conclude that the
    // whole log is in flight merely because the peer's progress is unknown.
    {
        let mut inner = cluster.inner.write().unwrap();
        let config = inner.config.clone();
        refresh_all_pipeline_states(&mut inner.nodes, 2, None, &config);
    }

    for target in [1u64, 3u64] {
        let request = cluster
            .build_append_entries_request(target)
            .unwrap_or_else(|err| panic!("new leader cannot even probe node {target}: {err}"));
        assert_eq!(request.leader_id, 2);
        assert_eq!(request.target_id, target);
    }
}


/// Concurrent appenders to one node log must SHARE durability barriers, not each take their own.
///
/// An fsync makes every byte already written to the file durable, so a barrier taken while other
/// writers are queued behind it covers them too. This was measured taking one barrier per write
/// no matter how many writers there were -- the fsync was held inside the cursor lock, so writers
/// could not even reach it at the same time, and sixteen writers paid sixteen barriers where one
/// would have done.
#[test]
fn concurrent_appends_to_one_node_log_share_barriers() {
    let dir = tempfile::tempdir().unwrap();
    // Any well-formed record will do -- this test is about how many barriers the appends cost,
    // not what they contain.
    let source = RaftCluster::new_single_shard_with_wal(
        dir.path().join("source"),
        1,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    source
        .propose(Command::StringSet { key: "seed".into(), value: b"seed".to_vec() })
        .unwrap();
    let record = source
        .inner
        .read()
        .unwrap()
        .wal_record_for(1)
        .expect("record for node 1")
        .1;

    let wal = std::sync::Arc::new(LocalRaftWal::new(dir.path().join("target")));
    let writers = 16usize;
    let each = 8usize;
    // Release every thread at the same instant, so they genuinely overlap rather than trickling
    // through one at a time and each finding no barrier to ride.
    let start = std::sync::Arc::new(std::sync::Barrier::new(writers));

    crate::durability_metrics::reset();
    let handles: Vec<_> = (0..writers)
        .map(|_| {
            let wal = std::sync::Arc::clone(&wal);
            let record = record.clone();
            let start = std::sync::Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                for _ in 0..each {
                    wal.persist_node_segmented(1, 1, &record, 1 << 20, 4).unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    let appends = (writers * each) as u64;
    let barriers = crate::durability_metrics::snapshot()
        .get("raft_wal_append")
        .copied()
        .unwrap_or(0);
    println!("{appends} concurrent appends cost {barriers} barriers");
    assert!(barriers >= 1, "some barrier must actually be taken");
    assert!(
        barriers < appends,
        "{barriers} barriers for {appends} concurrent appends -- nothing coalesced"
    );

    // Sharing a barrier must not cost anyone durability: every append is still on disk, and the
    // log still reads back cleanly.
    let recovered = wal.recover_node(1, 1).unwrap();
    assert!(
        recovered.valid_records > 0,
        "the node log must read back after concurrent appends"
    );
}


/// A propose that returned `Ok` must be on disk, however many proposers were running at once.
///
/// Making barriers coalesce moved the flush out from under the cluster locks, which put the
/// deferral bookkeeping -- shared owner/dirty flags -- in reach of two proposers at once. When
/// that happened, one proposer's "I owe a barrier" flag could be cleared by another opening its
/// own deferral, so the first staged nothing and returned success for a write that was never
/// written at all. Nothing about a single-threaded run shows that up.
#[test]
fn every_concurrent_propose_that_returned_ok_survives_a_restore() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = std::sync::Arc::new(
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap(),
    );
    cluster.set_local_node_id(1);

    let writers = 8usize;
    let each = 12usize;
    let start = std::sync::Arc::new(std::sync::Barrier::new(writers));
    let handles: Vec<_> = (0..writers)
        .map(|writer| {
            let cluster = std::sync::Arc::clone(&cluster);
            let start = std::sync::Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                let mut accepted = Vec::new();
                for index in 0..each {
                    let key = format!("durable-{writer:02}-{index:03}");
                    // Only keys whose propose actually returned Ok are claimed below. A refusal
                    // promises nothing and must not be held against the log.
                    if cluster
                        .propose(Command::StringSet {
                            key: key.clone(),
                            value: key.clone().into_bytes(),
                        })
                        .is_ok()
                    {
                        accepted.push(key);
                    }
                }
                accepted
            })
        })
        .collect();
    let accepted: Vec<String> = handles
        .into_iter()
        .flat_map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(accepted.len(), writers * each, "every propose should have been accepted");

    // Rebuild from nothing but the WAL. Anything acknowledged but never written is missing here.
    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    let logged: std::collections::BTreeSet<String> = restored
        .inner
        .read()
        .unwrap()
        .nodes
        .get(&1)
        .expect("node 1 present after restore")
        .log
        .clone()
        .into_iter()
        .filter_map(|entry| match entry.command {
            Command::StringSet { key, .. } => Some(key),
            _ => None,
        })
        .collect();
    let missing: Vec<&String> = accepted.iter().filter(|key| !logged.contains(*key)).collect();
    assert!(
        missing.is_empty(),
        "{} acknowledged writes are absent from the restored log, first: {:?}",
        missing.len(),
        missing.first()
    );
}


/// Replaying an append-only node log must not rescan everything accumulated so far.
///
/// Folding one incremental record used to scan the WHOLE running log twice -- once for entries a
/// conflict superseded, once for entries compaction removed. Each record carries about one new
/// entry, so the log grows by one per record and those scans cost 1 + 2 + 3 + ... over the
/// replay: quadratic in record count, meaning a long-lived node pays for its entire history every
/// time it starts. Nothing about a short log shows that up, and a stopwatch on a shared machine
/// measures the other tenants, so this asserts the scan is not entered at all.
#[test]
fn replaying_an_append_only_log_never_rescans_it() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster.set_local_node_id(1);
    let records = 400u64;
    for index in 0..records {
        cluster
            .propose(Command::StringSet {
                key: format!("append-{index:05}"),
                value: b"v".to_vec(),
            })
            .unwrap();
