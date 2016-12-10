// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Reconciliation between what the metaserver routes and what datanodes serve.
//!
//! Every datanode heartbeat carries `shard_states`: the shards that node is
//! actually serving right now. The metaserver keeps its own `shard -> owner`
//! map. Nothing has ever compared the two.
//!
//! So when the two drift apart the metaserver never finds out. A shard can
//! disappear from a node -- it was unloaded by an operator, it failed to reload
//! after a restart, a load was rolled back, a disk went read-only -- and the
//! owner map keeps pointing at that node indefinitely. The node is healthy, its
//! heartbeats never stop, so neither the stale-heartbeat detector nor reboot
//! detection has anything to say about it. Every read for that shard is routed
//! to a server that will miss on all of them, and the only signal is client-side
//! errors.
//!
//! [`compute_auto_rebalance`] cannot see it either: it evacuates shards whose
//! *owner* is unavailable, and this owner is perfectly available. It is the
//! shard that is gone, not the server.
//!
//! This module closes that loop. [`ShardChecker::check`] compares the owner map
//! against what each server reports and produces one
//! [`ShardReassignmentReason::OwnerDiverged`] reassignment per shard that its
//! recorded owner is not serving, so the existing rebalance loop re-places it
//! onto a node that will.
//!
//! Three guards keep the check from doing more harm than the drift:
//!
//! * **Reboot grace.** A node that restarted seconds ago has not finished
//!   reloading, so its shards are legitimately missing for a while. Servers
//!   inside [`ShardCheckOptions::reboot_grace_ms`] of their boot time are
//!   skipped -- otherwise every restart would trigger a full re-placement of
//!   everything that node owned.
//! * **Report capability.** A server is only judged once it has been *seen* to
//!   report shard states at least once
//!   ([`ServerMetaInfo::reports_shard_states`]). A node that never reports them
//!   is indistinguishable from one serving nothing, and treating silence as
//!   "serving nothing" would re-place the entire cluster.
//! * **Settle grace.** A shard the owner reports but has not finished loading
//!   is *in progress*, not missing. `serving_state` distinguishes them:
//!   `loading`, `reloading`, `queued`, `running` and `unloading` are transient,
//!   and acting on them would cancel work already underway and re-place a shard
//!   that was about to serve — thrashing the very load it is waiting on. Those
//!   states become actionable only after
//!   [`ShardCheckOptions::settle_grace_ms`] of being continuously reported that
//!   way. `serving` and `readonly` are healthy and never diverge; `failed` and
//!   `unloaded` are terminal and act immediately, as does a shard absent from
//!   the report entirely.
//! * **Rate limit.** At most [`ShardCheckOptions::max_moves_per_window`]
//!   divergences are acted on per window. A correlated fault can make many
//!   shards look missing at once, and reacting to all of them would move more
//!   data than the fault did.
//!
//! The comparison itself is pure; only the rate-limit window is stateful.

use std::collections::BTreeSet;

use super::*;

/// Tuning for [`ShardChecker`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCheckOptions {
    /// Skip a server this recently booted; it is still reloading its shards.
    pub reboot_grace_ms: u64,
    /// How long a shard may sit in a transient serving state before the
    /// metaserver stops waiting for it and treats it as diverged.
    pub settle_grace_ms: u64,
    /// Most divergences acted on per window.
    pub max_moves_per_window: usize,
    /// Length of the rate-limit window.
    pub window_ms: u64,
}

impl Default for ShardCheckOptions {
    fn default() -> Self {
        Self {
            reboot_grace_ms: 30_000,
            settle_grace_ms: 120_000,
            max_moves_per_window: 10,
            window_ms: 60_000,
        }
    }
}

/// One shard the metaserver routes to a server that is not serving it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardDivergence {
    pub shard_id: ShardId,
    /// The recorded owner, which is live but does not report this shard.
    pub server_addr: String,
    /// True when the owner reports the shard but is not serving it -- a weaker
    /// signal than the shard being absent from the report entirely.
    #[serde(default)]
    pub reported_unloaded: bool,
    /// The `serving_state` the owner reported, or empty when the shard was
    /// absent from the report altogether.
    #[serde(default)]
    pub serving_state: String,
}

/// What one reconciliation round found.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCheckReport {
    /// Divergences observed, ordered by shard id. Includes ones the rate limit
    /// declined to act on, so the full picture is always reportable.
    pub diverged: Vec<ShardDivergence>,
    /// Servers skipped because they booted too recently, ordered by address.
    pub skipped_in_reboot_grace: Vec<String>,
    /// Servers skipped because they have never reported shard states, ordered
    /// by address.
    pub skipped_without_shard_reports: Vec<String>,
    /// How many divergences the rate limit held back this round.
    pub rate_limited: usize,
    /// Shards being waited on: reported in a transient state that has not yet
    /// outlasted the settle grace. Not divergences, but worth surfacing --
    /// a shard that never leaves this list is a load that never finishes.
    #[serde(default)]
    pub settling: Vec<ShardId>,
}

/// How a datanode describes one shard it is holding, reduced to what the
/// reconciler needs. Ordered so the healthiest verdict wins when two workers
/// report the same shard mid-handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ShardHealth {
    /// The node has stopped trying (`failed`), or is holding it unloaded.
    Broken(&'static str),
    /// Work is underway: the node is loading, reloading, queued, running or
    /// unloading it. Not a divergence until it outlasts the settle grace.
    Transient(&'static str),
    /// Serving reads, in read-write or read-only mode.
    Serving(&'static str),
}

impl ShardHealth {
    fn classify(state: &ServerShardServingState) -> Self {
        match state.serving_state.as_str() {
            // `readonly` is a serving mode, not a fault: reads resolve there.
            "serving" | "readonly" => Self::Serving("serving"),
            "loading" | "reloading" | "queued" | "running" => Self::Transient("loading"),
            "unloading" => Self::Transient("unloading"),
            "failed" => Self::Broken("failed"),
            "unloaded" => Self::Broken("unloaded"),
            // An unrecognised state from a newer datanode: fall back to the
            // `loaded` flag rather than guessing it is broken.
            _ if state.loaded => Self::Serving("serving"),
            _ => Self::Broken("unloaded"),
        }
    }
}

/// The shards this server reports itself as actually serving.
///
/// Uses the same classification the divergence check does, so "serving" means
/// the same thing to both: a shard mid-load or one the node has given up on is
/// not being served, and `readonly` is.
pub fn serving_shards(server: &ServerMetaInfo) -> BTreeSet<ShardId> {
    server
        .shard_states
        .iter()
        .filter(|state| matches!(ShardHealth::classify(state), ShardHealth::Serving(_)))
        .map(|state| state.shard_id)
        .collect()
}

/// What one server looks like to the divergence check.
///
/// Taken while the metadata lock is held, so the check itself runs without it.
/// Copying the whole `ServerMetaInfo` to do that copied its shard-load list and
/// its shard-state list -- three strings for every shard a datanode holds --
/// for a check that reads neither. Every state is reduced to a [`ShardHealth`],
/// which is `Copy`, and that is what the check compares.
#[derive(Debug, Clone)]
pub struct ObservedServer {
    server_addr: String,
    state: MetaEntityState,
    reports_shard_states: bool,
    boot_time_ms: u64,
    /// The healthiest verdict per shard: two workers mid-handoff report the
    /// same shard, and one of them still serving it is enough.
    shard_health: BTreeMap<ShardId, ShardHealth>,
}

impl ObservedServer {
    pub fn observe(server: &ServerMetaInfo) -> Self {
        let mut shard_health: BTreeMap<ShardId, ShardHealth> = BTreeMap::new();
        for state in &server.shard_states {
            let health = ShardHealth::classify(state);
            let health = match shard_health.get(&state.shard_id) {
                Some(existing) => (*existing).max(health),
                None => health,
            };
            shard_health.insert(state.shard_id, health);
        }
        Self {
            server_addr: server.server_addr.clone(),
            state: server.state,
            reports_shard_states: server.reports_shard_states,
            boot_time_ms: server.boot_time_ms,
            shard_health,
        }
    }
}

/// Stateful reconciler: pure comparison plus a rate-limit window.
#[derive(Debug, Clone)]
pub struct ShardChecker {
    options: ShardCheckOptions,
    window_start_ms: u64,
    moves_this_window: usize,
    /// When each (shard, owner) pair was first seen in a transient state, so the
    /// settle grace is measured from the first observation rather than from a
    /// load start time the metaserver does not know.
    settling_since_ms: BTreeMap<(ShardId, String), u64>,
}

impl Default for ShardChecker {
    fn default() -> Self {
        Self::new(ShardCheckOptions::default())
    }
}

impl ShardChecker {
    pub fn new(options: ShardCheckOptions) -> Self {
        Self {
            options,
            window_start_ms: 0,
            moves_this_window: 0,
            settling_since_ms: BTreeMap::new(),
        }
    }

    pub fn options(&self) -> ShardCheckOptions {
        self.options
    }

    /// Divergences acted on in the current window.
    pub fn moves_this_window(&self) -> usize {
        self.moves_this_window
    }

    /// Compare the owner map against what each server reports serving.
    ///
    /// Pure apart from the rate-limit window: the same inputs produce the same
    /// report, and every output vector is sorted.
    pub fn check(
        &mut self,
        shard_owners: &BTreeMap<ShardId, String>,
        servers: &[ObservedServer],
        now_ms: u64,
    ) -> ShardCheckReport {
        self.roll_window(now_ms);

        let mut report = ShardCheckReport::default();
        // Which servers may be judged this round, and what each one serves.
        let mut served: BTreeMap<&str, &BTreeMap<ShardId, ShardHealth>> = BTreeMap::new();
        let mut judgeable: BTreeSet<&str> = BTreeSet::new();
        for server in servers {
            if server.state != MetaEntityState::Normal {
                // A frozen or dropped server is already being handled elsewhere;
                // its shards are not "diverged", they are unavailable.
                continue;
            }
            if !server.reports_shard_states {
                report
                    .skipped_without_shard_reports
                    .push(server.server_addr.clone());
                continue;
            }
            if self.in_reboot_grace(server, now_ms) {
                report.skipped_in_reboot_grace.push(server.server_addr.clone());
                continue;
            }
            judgeable.insert(server.server_addr.as_str());
            // Reduced when the server was observed, under the lock.
            served.insert(server.server_addr.as_str(), &server.shard_health);
        }
        report.skipped_in_reboot_grace.sort();
        report.skipped_without_shard_reports.sort();

        let mut still_settling = BTreeMap::new();
        for (shard_id, owner_addr) in shard_owners {
            if !judgeable.contains(owner_addr.as_str()) {
                continue;
            }
            let key = (*shard_id, owner_addr.clone());
            let health = served
                .get(owner_addr.as_str())
                .and_then(|shards| shards.get(shard_id))
                .copied();
            let (diverged, serving_state) = match health {
                // Serving, in either mode: nothing to reconcile.
                Some(ShardHealth::Serving(_)) => (false, None),
                Some(ShardHealth::Transient(state)) => {
                    // The node is working on it. Start (or continue) the clock,
                    // and only give up once the grace has elapsed -- acting
                    // sooner would cancel a load that was about to finish.
                    let since = self
                        .settling_since_ms
                        .get(&key)
                        .copied()
                        .unwrap_or(now_ms);
                    still_settling.insert(key.clone(), since);
                    if now_ms.saturating_sub(since) >= self.options.settle_grace_ms {
                        (true, Some(state))
                    } else {
                        report.settling.push(*shard_id);
                        (false, None)
                    }
                }
                // The node has stopped trying, or never had it.
                Some(ShardHealth::Broken(state)) => (true, Some(state)),
                None => (true, None),
            };
            if diverged {
                report.diverged.push(ShardDivergence {
                    shard_id: *shard_id,
                    server_addr: owner_addr.clone(),
                    reported_unloaded: serving_state.is_some(),
                    serving_state: serving_state.unwrap_or_default().to_string(),
                });
            }
        }
        // Forget clocks for shards that recovered or moved: a later stall must
        // start its own grace rather than inherit an expired one.
        self.settling_since_ms = still_settling;
        report.settling.sort();
        report.settling.dedup();
        report
    }

    /// Turn a report into reassignments, spending the window's budget. Each
    /// diverged shard moves to the least loaded live server that is not its
    /// current owner; a shard with nowhere else to go is left alone rather than
    /// pointed at the same node again.
    pub fn plan_moves(
        &mut self,
        report: &mut ShardCheckReport,
        shard_owners: &BTreeMap<ShardId, String>,
        live_servers: &BTreeSet<String>,
    ) -> Vec<ShardReassignment> {
        let mut moves = Vec::new();
        if live_servers.is_empty() {
            return moves;
        }
        let mut load: BTreeMap<String, usize> =
            live_servers.iter().map(|addr| (addr.clone(), 0)).collect();
        for owner_addr in shard_owners.values() {
            if let Some(count) = load.get_mut(owner_addr) {
                *count += 1;
            }
        }

        let mut rate_limited = 0_usize;
        let diverged = report.diverged.clone();
        for divergence in &diverged {
            if self.moves_this_window >= self.options.max_moves_per_window {
                rate_limited += 1;
                continue;
            }
            let Some(target) = load
                .iter()
                .filter(|(addr, _)| *addr != &divergence.server_addr)
                .min_by(|(addr_a, load_a), (addr_b, load_b)| {
                    load_a.cmp(load_b).then_with(|| addr_a.cmp(addr_b))
                })
                .map(|(addr, _)| addr.clone())
            else {
                // The diverging owner is the only live server. Re-placing onto
                // itself would achieve nothing; leave the route and let the
                // node reload.
                continue;
            };
            if let Some(count) = load.get_mut(&divergence.server_addr) {
                *count = count.saturating_sub(1);
            }
            *load.entry(target.clone()).or_default() += 1;
            self.moves_this_window += 1;
            moves.push(ShardReassignment {
                shard_id: divergence.shard_id,
                from_server: Some(divergence.server_addr.clone()),
                to_server: target,
                reason: ShardReassignmentReason::OwnerDiverged,
            });
        }
        report.rate_limited = rate_limited;
        moves
    }

    fn roll_window(&mut self, now_ms: u64) {
        let window = self.options.window_ms.max(1);
        if self.window_start_ms == 0 || now_ms.saturating_sub(self.window_start_ms) >= window {
            self.window_start_ms = now_ms;
            self.moves_this_window = 0;
        }
    }

    /// True while the server is still inside its post-boot reload window. A zero
    /// or future-dated boot time yields false: an unknown boot time must not
    /// grant an unbounded grace.
    fn in_reboot_grace(&self, server: &ObservedServer, now_ms: u64) -> bool {
        if self.options.reboot_grace_ms == 0 || server.boot_time_ms == 0 {
            return false;
        }
        now_ms
            .checked_sub(server.boot_time_ms)
            .is_some_and(|age| age < self.options.reboot_grace_ms)
    }
}

impl SingleNodeMeta {
    /// Run one reconciliation round against the current state and return both
    /// the findings and the reassignments to drive.
    pub fn check_shard_divergence(
        &self,
        checker: &mut ShardChecker,
    ) -> (ShardCheckReport, Vec<ShardReassignment>) {
        let now = now_ms();
        let (shard_owners, servers, live_servers) = {
            let state = self.inner.read().expect("meta lock poisoned");
            // A frozen shard is out of service on purpose, so its owner not
            // serving it is expected rather than divergence.
