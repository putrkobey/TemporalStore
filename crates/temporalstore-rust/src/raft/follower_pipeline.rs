// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! One ordered sender per follower.
//!
//! Replication used to fan out from two independent places -- each propose spawned its own
//! threads, and the timer loop sent heartbeats and catch-up batches on its own -- so two appends
//! to the same follower could be in flight at once, and under real network latency they arrived
//! out of order. The follower's rejections then did, badly, the job a sender-side window should
//! do: measured on a live cluster, the interleaving drove election churn and snapshot storms
//! that no loopback test reproduces, because loopback round trips never let the sends overlap.
//!
//! This module gives every follower exactly one sender thread, and every append to that follower
//! goes through it: propose-driven entries, catch-up batches, heartbeats, and the snapshot
//! fallback. Sends are synchronous, so at most one append is in flight per follower and order
//! holds by construction; throughput comes from batching, not pipelining depth. Proposers no
//! longer replicate at all -- they append to the leader's log, ring the senders, and wait for
//! the quorum commit signal. The commit index reaches followers on the next append or heartbeat
//! instead of a dedicated second round trip per propose.
//!
//! The senders also carry the liveness bookkeeping that used to ride the timer loop's own sends
//! (a peer marked dead after consecutive failures, alive again on any success, a stand-down when
//! a response names a newer term). If they did not, the timer loop would still have to probe
//! peers itself -- and two probes to one follower is exactly the interleaving this exists to end.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use super::*;

/// How long a sender sleeps after a failed send, so an unreachable peer costs retries on this
/// cadence rather than a busy loop.
const SEND_FAILURE_BACKOFF_MS: u64 = 50;

/// Consecutive rejections without progress before the sender concludes the log cannot reach this
/// peer and installs a snapshot instead. Rejections walk `next_index` back one probe at a time,
/// so a peer that is merely behind converges well before this.
const REJECTIONS_BEFORE_SNAPSHOT: u32 = 8;

#[derive(Default)]
struct PeerWake {
    /// New log entries may be waiting for this peer.
    entries: bool,
    /// A heartbeat is due whether or not anything is pending.
    heartbeat: bool,
    /// This sender generation is shutting down.
    stop: bool,
}

/// The doorbell for one follower's sender thread.
struct PeerSignal {
    wake: Mutex<PeerWake>,
    changed: Condvar,
}

impl PeerSignal {
    fn new() -> Self {
        Self {
            wake: Mutex::new(PeerWake::default()),
            changed: Condvar::new(),
        }
    }

    fn ring_entries(&self) {
        let mut wake = self.wake.lock().expect("peer signal poisoned");
        wake.entries = true;
        self.changed.notify_one();
    }

    fn ring_heartbeat(&self) {
        let mut wake = self.wake.lock().expect("peer signal poisoned");
        wake.heartbeat = true;
        self.changed.notify_one();
    }

    fn ring_stop(&self) {
        let mut wake = self.wake.lock().expect("peer signal poisoned");
        wake.stop = true;
        self.changed.notify_all();
    }

    /// Block until something rings, then take and clear the reasons.
    fn wait(&self) -> (bool, bool) {
        let mut wake = self.wake.lock().expect("peer signal poisoned");
        while !(wake.entries || wake.heartbeat || wake.stop) {
            wake = self.changed.wait(wake).expect("peer signal poisoned");
        }
        let out = (wake.heartbeat, wake.stop);
        wake.entries = false;
        wake.heartbeat = false;
        out
    }
}

/// What the sender knows about one peer's reachability, read by check-quorum.
pub(crate) struct PeerHealth {
    /// Milliseconds since pipeline start of the last successful exchange; 0 = never.
    last_ok_at_ms: AtomicU64,
    consecutive_failures: AtomicU64,
}

/// The per-follower senders for one cluster.
pub(crate) struct FollowerPipeline {
    signals: BTreeMap<RaftNodeId, Arc<PeerSignal>>,
    health: BTreeMap<RaftNodeId, Arc<PeerHealth>>,
    /// The peer set the senders were built for; a change rebuilds the pipeline.
    peer_set: Vec<RaftNodeId>,
    started: Instant,
    stopping: Arc<AtomicBool>,
}

impl FollowerPipeline {
    fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn ring_entries(&self) {
        for signal in self.signals.values() {
            signal.ring_entries();
        }
    }

    fn ring_heartbeat(&self) {
        for signal in self.signals.values() {
            signal.ring_heartbeat();
        }
    }

    pub(crate) fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        for signal in self.signals.values() {
            signal.ring_stop();
        }
    }

    /// How many peers answered a send within the last `window_ms`. Check-quorum counts these
    /// plus the leader itself.
    fn reached_within_ms(&self, window_ms: u64) -> usize {
        let now = self.now_ms();
        self.health
            .values()
            .filter(|health| {
                let at = health.last_ok_at_ms.load(Ordering::Relaxed);
                at > 0 && now.saturating_sub(at) <= window_ms
            })
            .count()
    }
}

impl RaftCluster {
    /// Bring the per-follower senders up, or rebuild them after the peer set changed. Cheap when
    /// already current: one lock and a comparison.
    pub(crate) fn ensure_follower_pipeline<T>(&self, transport: &T)
    where
        T: RaftTransport + Clone + Send + 'static,
    {
        let (local, mut wanted) = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let ids: Vec<RaftNodeId> = inner.nodes.keys().copied().collect();
            (inner.local_node_id, ids)
        };
        if let Some(local) = local {
            wanted.retain(|id| *id != local);
        } else {
            // The in-process topology has no single local node; the leader may be any of them.
            // Every node gets a sender, and a sender simply idles while its target is the
            // leader itself.
            let leader = {
                let inner = self.inner.read().expect("raft cluster lock poisoned");
                inner.leader_id
            };
            wanted.retain(|id| *id != leader);
        }
        let mut pipeline = self.follower_pipeline.lock().expect("pipeline lock poisoned");
        if let Some(existing) = pipeline.as_ref() {
            if existing.peer_set == wanted {
                return;
            }
            existing.stop();
        }
        let stopping = Arc::new(AtomicBool::new(false));
        let mut signals = BTreeMap::new();
        let mut health = BTreeMap::new();
        let started = Instant::now();
        for peer in &wanted {
            let signal = Arc::new(PeerSignal::new());
            let peer_health = Arc::new(PeerHealth {
                last_ok_at_ms: AtomicU64::new(0),
                consecutive_failures: AtomicU64::new(0),
            });
            signals.insert(*peer, Arc::clone(&signal));
            health.insert(*peer, Arc::clone(&peer_health));
            let cluster = self.clone();
            let transport = transport.clone();
            let peer = *peer;
            let stopping = Arc::clone(&stopping);
            std::thread::spawn(move || {
                sender_loop(cluster, transport, peer, signal, peer_health, stopping, started)
            });
        }
        *pipeline = Some(FollowerPipeline {
            signals,
            health,
            peer_set: wanted,
            started,
            stopping,
        });
    }

    /// Wake every sender: new entries are in the log.
    pub(crate) fn ring_replication(&self) {
        if let Some(pipeline) = self
            .follower_pipeline
            .lock()
            .expect("pipeline lock poisoned")
            .as_ref()
        {
            pipeline.ring_entries();
        }
    }

    /// Wake every sender for a heartbeat round.
    pub(crate) fn ring_heartbeats(&self) {
        if let Some(pipeline) = self
            .follower_pipeline
            .lock()
            .expect("pipeline lock poisoned")
            .as_ref()
        {
            pipeline.ring_heartbeat();
        }
    }

    /// Renew the leader lease after the timer loop confirmed quorum contact through the
    /// senders. The fan-out's own heartbeat round renews the lease as acks come back, but
    /// sender heartbeats fold their acks into the commit advance, which renews only when a
    /// commit actually moves -- so an IDLE leader's lease decayed, and the first propose
    /// after any gap longer than the lease bounced off `leader_lease_valid` until something
    /// committed. On hardware that surfaced as ~400 ms per propose on every cadence slower
    /// than the lease window, while busy cells never saw it.
    pub(crate) fn renew_leader_lease_after_quorum_contact(&self) {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let leader_id = inner.leader_id;
        let is_leader = inner
            .nodes
            .get(&leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false);
        if is_leader {
            inner.renew_leader_lease();
        }
    }

    /// Peers that answered within `window_ms`, for check-quorum. The leader counts itself.
    pub(crate) fn pipeline_reached_within(&self, window_ms: u64) -> usize {
        self.follower_pipeline
            .lock()
            .expect("pipeline lock poisoned")
            .as_ref()
            .map(|pipeline| pipeline.reached_within_ms(window_ms))
            .unwrap_or(0)
    }

    /// Block until the quorum commit reaches `index`, or the deadline passes.
    pub(crate) fn wait_for_quorum_commit(&self, index: u64, deadline: Duration) -> bool {
        let started = Instant::now();
