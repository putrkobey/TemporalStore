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
    }

    let wal = LocalRaftWal::new(dir.path());
    crate::durability_metrics::reset();
    let recovered = wal.recover_node(1, 1).unwrap();
    let counts = crate::durability_metrics::snapshot();

    // Every record here is a plain append: it supersedes nothing and compacts nothing, so the
    // trims have no work to do and must not walk the accumulated log to discover that. Asserting
    // on entries EXAMINED rather than on a branch counter is what makes this catch a regression:
    // an implementation that goes back to scanning unconditionally reports its scan here.
    let scanned = counts.get("replay_entries_scanned").copied().unwrap_or(0);
    println!("replay of {records} append-only records examined {scanned} accumulated entries");
    assert_eq!(
        scanned, 0,
        "replaying {records} plain appends examined {scanned} accumulated entries;          scanning the running log per record makes replay quadratic in record count"
    );

    // ...and the replay must still be right, not merely cheap.
    let record = recovered.record.expect("a record is recovered");
    assert_eq!(
        record.entries.len() as u64,
        records,
        "every proposed entry should survive replay"
    );
    for (position, entry) in record.entries.iter().enumerate() {
        match &entry.command {
            Command::StringSet { key, .. } => {
                assert_eq!(key, &format!("append-{position:05}"), "entries stay in log order")
            }
            other => panic!("unexpected command in the replayed log: {other:?}"),
        }
    }
}


/// Building an AppendEntries must cost what it SENDS, not what the log already holds.
///
/// The entries a peer still needs sit above its `prev_log_index`, and a raft log is in ascending
/// index order, so they are a suffix. Finding that suffix by iterating from index 0 and filtering
/// costs the whole log on every AppendEntries to every peer -- so appending one entry got steadily
/// more expensive as history accumulated, which is one of the ways write latency ends up growing
/// with log length rather than staying flat.
#[test]
fn building_an_append_entries_does_not_walk_the_whole_log() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster.set_local_node_id(1);
    let entries = 500u64;
    for index in 0..entries {
        cluster
            .propose(Command::StringSet {
                key: format!("hist-{index:05}"),
                value: b"v".to_vec(),
            })
            .unwrap();
    }

    // Put the peer fully caught up, so the next request carries nothing at all: the ideal case,
    // and the one that used to cost a full scan of every entry ever appended.
    {
        let mut inner = cluster.inner.write().unwrap();
        let last = inner
            .nodes
            .get(&1)
            .map(|node| node.log.last().map(|entry| entry.index).unwrap_or(0))
            .unwrap_or(0);
        if let Some(peer) = inner.nodes.get_mut(&2) {
            peer.pipeline_state.next_index = last + 1;
            peer.pipeline_state.inflight_entries = 0;
            peer.pipeline_state.inflight_bytes = 0;
        }
    }

    crate::durability_metrics::reset();
    let request = cluster.build_append_entries_request(2).unwrap();
    let examined = crate::durability_metrics::snapshot()
        .get("replication_entries_examined")
        .copied()
        .unwrap_or(0);
    println!(
        "with {entries} entries in the log, a caught-up peer's request examined {examined} and carried {}",
        request.entries.len()
    );
    assert!(
        request.entries.is_empty(),
        "a caught-up peer should be sent nothing"
    );
    assert!(
        examined < entries,
        "building a request for a caught-up peer examined {examined} of {entries} entries; \
         the cost of a request must not grow with the history behind it"
    );
}


/// A follower that is behind and not converging must not report itself as caught up.
///
/// Staleness used to be derivable only by comparing log indices against this process's own shadow
/// of the peer, and that shadow does not move when the peer rejects -- so a follower that had
/// stopped converging reported a lag of ZERO while falling further behind. The failure mode
/// reported itself as health, which is why it went unnoticed. Time cannot be faked the same way:
/// the comparison is against the commit index the LEADER sent, and the clock advances whether or
/// not appends are landing.
#[test]
fn a_follower_that_stops_converging_cannot_report_zero_lag() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster.set_local_node_id(2);

    let accepted = |index: u64| AppendEntriesRequest {
        rpc: None,
        shard_id: 1,
        term: 1,
        leader_id: 1,
        target_id: 2,
        prev_log_index: index - 1,
        prev_log_term: if index == 1 { 0 } else { 1 },
        entries: vec![RaftLogEntry {
            leader_time_ms: 0,
            term: 1,
            index,
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k-{index}"),
                value: b"v".to_vec(),
            },
        }],
        leader_commit: index,
    };

    // Land two entries, so the follower is genuinely caught up with what the leader has committed.
    cluster.receive_append_entries(accepted(1)).unwrap();
    cluster.receive_append_entries(accepted(2)).unwrap();
    cluster.advance_time_ms(30_000);
    assert_eq!(
        cluster.replication_stall_ms(2),
        0,
        "a caught-up follower must not look stalled just because time passed"
    );

    // Now the leader moves on, but every append this follower is offered refers to a log position
    // it does not have, so it rejects them all -- while still hearing from the leader. This is
    // exactly the shape that used to report lag 0.
    let mismatched = |index: u64| AppendEntriesRequest {
        prev_log_index: index + 500,
        prev_log_term: 1,
        ..accepted(index)
    };
    for index in 3..8 {
        let response = cluster.receive_append_entries(mismatched(index)).unwrap();
        assert!(!response.success, "the mismatched append should be rejected");
        cluster.advance_time_ms(10_000);
    }

    let stalled = cluster.replication_stall_ms(2);
    assert!(
        stalled >= 50_000,
        "a follower behind the leader and accepting nothing reported a stall of {stalled} ms; \
         it has not made progress for the whole run"
    );

    // Once it accepts again, the stall clears -- it is a progress signal, not a one-way latch.
    cluster.receive_append_entries(accepted(3)).unwrap();
    assert_eq!(
        cluster.replication_stall_ms(2),
        0,
        "a follower that has caught up must report no stall"
    );
}


/// The AUTHENTICATED http entry point must accept a binary append body.
///
/// It extracts the rpc auth metadata before dispatching, and it did that by parsing the body as
/// JSON -- so with binary replication on, every append got a 403 from the auth wrapper and the
/// cluster could not replicate at all. The unauthenticated handler (which the other tests use)
/// was fine, which is exactly why this test targets the authenticated one.
#[test]
fn authenticated_route_accepts_a_binary_append() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    cluster.set_local_node_id(2);
    let request = AppendEntriesRequest {
        rpc: None,
        shard_id: 1,
        term: 1,
        leader_id: 1,
        target_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![RaftLogEntry {
            leader_time_ms: 0,
            term: 1,
            index: 1,
            shard_id: 1,
            command: Command::StringSet {
                key: "auth".into(),
                value: b"binary".to_vec(),
            },
        }],
        leader_commit: 0,
    };
    let body = wal_proto::encode_append_entries(&request).unwrap();
    let (code, response) = handle_authenticated_raft_http(
        &cluster,
        HttpRequest {
            method: "POST".to_string(),
            path: "/raft/append_entries".to_string(),
            body,
        },
        "",
    );
    assert_eq!(
        code, 200,
        "the authenticated route rejected a binary append: {}",
        String::from_utf8_lossy(&response)
    );
    let parsed: AppendEntriesResponse = serde_json::from_slice(&response).unwrap();
    assert!(parsed.success, "the append must actually be accepted");
}

/// The majority check is the ONLY thing stopping a cut-off node serving its own frozen bar.
///
/// `read_from_replica` decides freshness by comparing this node's indices against
/// `inner.nodes[inner.leader_id].commit_index` -- a value this process maintains itself, from
/// what the leader last told it. When the peers go away that value FREEZES, and a frozen bar
/// that the node already satisfies can never refuse. What actually refuses is the separate
/// majority check in `read_index`.
///
/// That is a single point of failure for a consistency property, so this pins it. The test is
/// written as two arms so it names what is load-bearing instead of merely passing: with the
/// majority intact the read is allowed, and when the ONLY thing taken away is the majority --
/// no index moves -- the read must be refused. It also asserts, in that same state, that
/// `read_from_replica` on its own still serves, which is the evidence that the bar is not what
/// is protecting anything.
///
/// This is the second instance in this codebase of a node comparing against a value it
/// maintains itself, with the comparison unable to fail: the first was a restarted replica
/// whose health check took its own frozen index as the thing to beat, reported `lag: 0`, and
/// answered that committed keys did not exist. Treat the shape as a pattern, not a one-off.
#[test]
fn only_the_majority_check_refuses_a_read_on_a_node_that_lost_its_peers() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "frozen-bar".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();

    let before = cluster.status();
    let node3 = |status: &RaftClusterStatus| {
        status
            .nodes
            .iter()
            .find(|n| n.node_id == 3)
            .expect("node 3 must be in the status")
            .clone()
    };
    let n3_before = node3(&before);
    // The bar `read_from_replica` uses is the leader's commit index as THIS process holds it.
    // Everything is caught up, so node 3 already satisfies it -- the bar cannot refuse.
    assert!(
        before.has_majority,
        "the baseline needs a majority, or the arms below are not isolating it: {before:?}"
    );
    assert!(
        n3_before.applied_index >= before.commit_index,
        "node 3 must be fully applied before the majority is taken away, or a refusal could \
         come from lag rather than from the majority check: node3={n3_before:?} \
         cluster_commit={}",
        before.commit_index
    );

    // Arm 1 -- majority intact: the read is ALLOWED. Without this the refusal below could be
    // agreeing with nothing.
    assert!(
        cluster.read_index(3).is_ok(),
        "with a majority, the read index must be granted"
    );
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "frozen-bar".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        },
        "with a majority, node 3 must serve the committed value"
    );

    // Arm 2 -- take away ONLY the majority. No index moves; the bar stays exactly where it was,
    // which is what being cut off does to it.
    cluster.set_alive(1, false).unwrap();
    cluster.set_alive(2, false).unwrap();
    let after = cluster.status();
    let n3_after = node3(&after);
    assert!(
        !after.has_majority,
        "taking two of three voters away must drop the majority: {after:?}"
    );
    assert_eq!(
        (n3_after.commit_index, n3_after.applied_index),
        (n3_before.commit_index, n3_before.applied_index),
        "no index may move between the arms, or this test is not isolating the majority check"
    );

    // The bar on its own does NOT refuse: it is frozen at a value node 3 already satisfies.
    // This is the finding the guard exists to pin, not an incidental detail.
    assert!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "frozen-bar".to_string()
                },
            )
            .is_ok(),
        "the freshness bar is expected NOT to refuse here -- it is a value this node maintains \
         itself and it froze when the peers went away. If this ever starts refusing, the bar has \
         become load-bearing and the comment above is stale."
    );

    // ...and the read index refuses.
    let before_counts = cluster.read_safety_runtime_state();
    assert!(
        matches!(cluster.read_index(3), Err(RaftError::LeaderUnavailable)),
        "a node that has lost its majority must be refused the read index. Got: {:?}",
        cluster.read_index(3)
    );

    // The refusal ALONE does not pin the majority check, and asserting only `is_err()` here was
    // measured to be worthless: with `has_majority` forced true, this test still PASSED, because
    // the stale leader lease refuses in the same breath. Two independent maintainers, so a
    // single-removal plant proves nothing -- "not caught" meant REDUNDANT, not wrong.
    //
    // So assert the ATTRIBUTION. The minority counter only moves when the decision was reached
    // on the majority, which is what makes this guard about `has_majority` rather than about the
    // node merely saying no.
    let after_counts = cluster.read_safety_runtime_state();
    assert!(
        after_counts.minority_partition_read_rejected
            > before_counts.minority_partition_read_rejected,
        "the refusal must be ATTRIBUTED to the lost majority, not merely happen. \
         minority_partition_read_rejected went {} -> {}. If this is flat while the read still \
         fails, something else is refusing and the majority check has quietly stopped being \
         what protects a cut-off node.",
        before_counts.minority_partition_read_rejected,
        after_counts.minority_partition_read_rejected
    );
}

/// Which peer a request came from must be answerable for BOTH encodings of an append.
///
/// The sibling above fixed the auth wrapper, which failed loudly: a JSON parse over a binary body
/// errored, and every append got a 403. A caller that only wants the sender id fails quietly
/// instead -- `None`, which reads as "no peer to filter" and therefore allows. That is how the
/// three-node chaos harness's peer block stopped covering replication on the day appends became
/// binary: the harness went on reporting that it had cut a node off while the leader kept
/// replicating to it, so an "isolated" follower was in fact fully current and served reads
/// correctly. The harness failed on every run from then on, and nothing ran it.
#[test]
fn the_sender_of_an_append_is_attributed_in_both_encodings() {
    let request = AppendEntriesRequest {
        rpc: None,
        shard_id: 1,
        term: 7,
        leader_id: 1,
        target_id: 3,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![RaftLogEntry {
            leader_time_ms: 0,
            term: 7,
            index: 1,
            shard_id: 1,
            command: Command::StringSet {
                key: "sender".into(),
                value: b"attributed".to_vec(),
            },
        }],
        leader_commit: 0,
    };

    let json_body = serde_json::to_vec(&request).unwrap();
    assert_eq!(
        replication_peer_id_for_http_request(&HttpRequest {
            method: "POST".to_string(),
            path: "/raft/append_entries".to_string(),
            body: json_body,
        }),
        Some(1),
        "a JSON append must be attributed to its leader"
    );

    let binary_body = wal_proto::encode_append_entries(&request).unwrap();
    assert!(
        is_binary_rpc(&binary_body),
        "the encoder must actually have produced the binary shape, or this proves nothing"
    );
    assert_eq!(
        replication_peer_id_for_http_request(&HttpRequest {
            method: "POST".to_string(),
            path: "/raft/append_entries".to_string(),
            body: binary_body,
        }),
        Some(1),
        "a BINARY append must be attributed to the same leader; returning None here reads as \
         'no peer to filter' and silently disables every sender-keyed filter"
    );

    let vote = VoteRequest {
        rpc: None,
        shard_id: 1,
        term: 7,
        candidate_id: 2,
        target_id: 3,
        last_log_index: 0,
        last_log_term: 0,
        pre_vote: false,
    };
    assert_eq!(
        replication_peer_id_for_http_request(&HttpRequest {
            method: "POST".to_string(),
            path: "/raft/request_vote".to_string(),
            body: serde_json::to_vec(&vote).unwrap(),
        }),
        Some(2),
        "a vote must be attributed to its candidate"
    );
}


/// Counts how many appends are in flight per peer at once, failing the moment two overlap.
///
/// Two appends in flight to one follower is the disease the per-follower senders exist to cure:
/// under real network latency the overlapping sends arrive out of order, the follower rejects,
/// and the retries snowball into election churn. Loopback never shows it -- so this transport
/// makes the overlap itself the assertion, latency or no latency.
#[derive(Clone)]
struct OneInFlightTransport {
    cluster: RaftCluster,
    in_flight: std::sync::Arc<
        std::sync::Mutex<std::collections::BTreeMap<RaftNodeId, u32>>,
    >,
    max_seen: std::sync::Arc<std::sync::atomic::AtomicU32>,
}

impl OneInFlightTransport {
    fn enter(&self, peer: RaftNodeId) {
        let mut map = self.in_flight.lock().unwrap();
        let slot = map.entry(peer).or_insert(0);
        *slot += 1;
        self.max_seen
            .fetch_max(*slot, std::sync::atomic::Ordering::SeqCst);
    }
    fn leave(&self, peer: RaftNodeId) {
        let mut map = self.in_flight.lock().unwrap();
        *map.entry(peer).or_insert(1) -= 1;
    }
}

impl RaftTransport for OneInFlightTransport {
    fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let peer = request.target_id;
        self.enter(peer);
        // Hold the request open a moment so a second concurrent sender WOULD overlap here.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let out = self.cluster.receive_append_entries(request);
        self.leave(peer);
        out
    }
    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        self.cluster.receive_vote_request(request)
    }
    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.cluster.receive_install_snapshot(request)
    }
    fn install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        self.cluster.receive_install_snapshot_chunk(request)
    }
}

/// Eight concurrent proposers, and never two appends in flight to one follower.
#[test]
fn at_most_one_append_in_flight_per_follower() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    // This test IS the pipeline's invariant; it must not silently test the default.
    cluster.use_follower_pipeline_for_test();
    let transport = OneInFlightTransport {
        cluster: cluster.clone(),
