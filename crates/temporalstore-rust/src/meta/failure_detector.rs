// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Adaptive heartbeat failure detection and correlated-failure conviction gating.
//!
//! The metaserver decides that a datanode is dead in
//! [`SingleNodeMeta::freeze_stale_resources_with_policy`] (`meta/meta_lifecycle.rs`):
//! any server whose last heartbeat is older than a fixed `stale_after_ms` is
//! frozen. That single fixed threshold has three failure modes this module
//! addresses, all of which turn a small incident into a cluster-wide one:
//!
//! 1. **It is not adaptive.** One threshold is applied to every server no matter
//!    how often that server actually heartbeats. Tuned tight enough to detect a
//!    1s-interval node quickly, it false-positives on a node with a longer
//!    interval or a slower link; tuned loose enough to be safe for the slow
//!    node, detection of the fast node is needlessly late.
//! 2. **A metaserver stall convicts the whole fleet.** `stale_after_ms` is
//!    measured against wall clock, so if the metaserver itself pauses (GC,
//!    scheduling starvation, a slow snapshot install) every server crosses the
//!    threshold at once. The first round after the stall then freezes every
//!    datanode, even though nothing was ever wrong with them.
//! 3. **A correlated failure is treated as N independent ones.** Losing a rack
//!    or a switch makes every server behind it look stale simultaneously.
//!    Freezing them all drains the topology, and the freeze is what
//!    [`compute_auto_rebalance`] and [`compute_raft_failover_triggers`] act on,
//!    so the metaserver amplifies a partial outage into a total one.
//!
//! This module supplies the two mechanisms the fixed threshold lacks:
//!
//! * [`MetaFailureDetector`] is a phi-accrual detector. It learns each server's
//!   own heartbeat interval distribution and reports suspicion as
//!   `phi = -log10(P(still alive | silence so far))` under an exponential
//!   arrival model, so one [`FailureDetectorOptions::phi_failure_threshold`]
//!   means the same confidence for a fast node and a slow one. It also holds a
//!   stall guard: if the detector itself did not run for a while it reports
//!   [`Diagnosis::Unknown`] and suppresses conviction for a grace window rather
//!   than blaming the fleet for its own pause (failure mode 2).
//! * [`plan_conviction`] is a correlated-failure gate. Servers are grouped by
//!   [`ServerMetaInfo::location`], and if the abnormal fraction of a location
//!   exceeds a warning/critical ratio the whole location enters *safe mode*: no
//!   server in it is convicted this round, and the severity is reported so an
//!   operator can act. An isolated failure still convicts immediately, which is
//!   what keeps failure mode 3 from costing detection speed.
//!
//! Both are pure and deterministic given an injected clock, so the whole
//! decision is unit-testable without a running cluster.
//! [`MetaFailureDetector::plan_round`] wires them together into the single call
//! the metaserver loop makes.
//!
//! Both datanodes and proxies are judged this way. Proxies were initially left
//! on the fixed threshold on the grounds that freezing one is cheap because it
//! moves no data. That reasoning covers failure mode 1 and misses the other two,
//! which are what actually matter here: a metaserver stall or a rack fault
//! freezes *every* proxy at once, and the proxies are the routing tier, so
//! losing all of them takes out the serving path completely. The blast radius of
//! a correlated proxy failure is larger than for datanodes, not smaller.
//!
//! The detector is fed by polling [`ServerMetaInfo::last_heartbeat_ms`] rather
//! than by hooking the heartbeat handler: that field already carries the true
//! arrival timestamp, so the learned interval distribution is identical, while
//! the heartbeat hot path takes no extra lock.

use std::collections::BTreeSet;

use super::*;

/// log10(e). Converts the exponential-model silence ratio into a phi value, so
/// phi reads as "how many nines of confidence that the node is gone".
const LOG10_E: f64 = std::f64::consts::LOG10_E;

/// Tuning for [`MetaFailureDetector`].
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FailureDetectorOptions {
    /// How many recent heartbeat intervals per server feed the mean.
    pub sample_capacity: usize,
    /// Interval assumed for a server's very first heartbeat, before any real
    /// interval has been observed. Deliberately large: it biases early phi
    /// downward, so a freshly registered server is never convicted on the
    /// strength of a single sample.
    pub initial_interval_ms: u64,
    /// Intervals longer than this are dropped instead of folded into the mean.
    /// They record a stall or a clock artifact, not the node's cadence, and
    /// admitting them would inflate the mean and blind the detector.
    pub max_interval_ms: u64,
    /// Convict when phi exceeds this. 5.0 means roughly "the odds of this much
    /// silence from a live node are about 1 in 10^5".
    pub phi_failure_threshold: f64,
    /// If the detector itself did not run for longer than this, suspicion is
    /// suppressed for the same duration again (the stall guard).
    pub max_round_pause_ms: u64,
}

impl Default for FailureDetectorOptions {
    fn default() -> Self {
        Self {
            sample_capacity: 1000,
            initial_interval_ms: 30_000,
            max_interval_ms: 60_000,
            phi_failure_threshold: 5.0,
            max_round_pause_ms: 5_000,
        }
    }
}

/// What the detector believes about one server right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Diagnosis {
    /// Suspicion is not trustworthy this round (the detector itself stalled, or
    /// is still inside the grace window after a stall). Never convict.
    Unknown,
    /// No heartbeat has ever been observed for this server, so there is no
    /// interval distribution to judge it against. Never convict.
    NotObserved,
    /// Silence so far is consistent with this server's own heartbeat cadence.
    Healthy,
    /// Silence exceeds [`FailureDetectorOptions::phi_failure_threshold`].
    Failed,
}

/// How badly one location is damaged, from the fraction of its servers that are
/// abnormal. Ordered: `Normal` < `Warning` < `Critical`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DamageSeverity {
    Normal,
    Warning,
    Critical,
}

/// Tuning for the correlated-failure gate in [`plan_conviction`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvictionPolicy {
    /// Abnormal percentage of a location above which it is `Warning`.
    pub warning_ratio_percent: u64,
    /// Abnormal percentage of a location above which it is `Critical`.
    pub critical_ratio_percent: u64,
    /// A location needs at least this many abnormal servers before safe mode can
    /// engage at all. Safe mode exists to catch *correlated* failure; without
    /// this floor a percentage alone would put a small cluster into safe mode on
    /// its first single dead node and then never convict anything.
    pub min_abnormal_for_safe_mode: usize,
    /// Master switch for the correlated-failure gate. With it off, every failed
    /// server is convicted regardless of how many of its neighbours also failed.
    pub safe_mode_enabled: bool,
    /// Master switch for conviction itself. With it off the plan is still
    /// computed and reportable but no server is named for freezing (dry run).
    pub convict_enabled: bool,
    /// Treat a detected reboot as a failure. A restarted datanode has dropped
    /// every shard the metaserver still believes it is serving, so leaving it in
    /// the topology means routing reads to a node that will miss on all of them.
    /// With this off a reboot is reported but not acted on.
    pub convict_on_reboot: bool,
    /// Convict proxies as well as datanodes. Kept separate because the two
    /// tiers fail for different reasons and an operator may want to hold the
    /// routing tier steady while letting datanode conviction run.
    pub convict_proxies: bool,
    /// Refuse to convict a server when doing so would leave some shard with no
    /// live server serving it.
    ///
    /// Conviction is decided entirely on heartbeat evidence, which says nothing
    /// about what the server is holding. Freezing the last live holder of a
    /// shard makes that shard unroutable, and then hands auto-rebalance a shard
    /// to "recover" onto a node that has none of its data. Off by default: a
    /// node that is genuinely gone is not serving the shard either, and holding
    /// the conviction back keeps a dead node in the topology.
    pub forbid_orphaning_shards: bool,
}

impl Default for ConvictionPolicy {
    fn default() -> Self {
        Self {
            warning_ratio_percent: 5,
            critical_ratio_percent: 8,
            min_abnormal_for_safe_mode: 2,
            safe_mode_enabled: true,
            convict_enabled: true,
            convict_on_reboot: true,
            convict_proxies: true,
            forbid_orphaning_shards: false,
        }
    }
}

/// One server as seen by [`plan_conviction`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvictionCandidate {
    pub server_addr: String,
    /// Location tag used to group servers for the correlated-failure gate.
    /// Servers with an empty location share one implicit group.
    pub location: String,
    /// The server is not currently serving (already frozen). It counts toward
    /// its location's damage but is not convicted again.
    pub abnormal: bool,
    /// The detector diagnosed this serving server as [`Diagnosis::Failed`].
    pub failed: bool,
    /// The server restarted in place: it heartbeated a boot time different from
    /// the one the metaserver anchored on. Unlike `failed` this is not inferred
    /// from silence, so it is trustworthy even while the detector is paused.
    #[serde(default)]
    pub rebooted: bool,
    /// Shards this candidate is currently serving, used by the orphan guard to
    /// work out what freezing it would cost. Empty for proxies, which serve no
    /// shards.
    #[serde(default)]
    pub serving_shards: Vec<ShardId>,
}

/// The damage assessment for one location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocationDamage {
    pub location: String,
    pub severity: DamageSeverity,
    pub total_servers: usize,
    pub abnormal_servers: usize,
    /// True when this location is held back from conviction this round.
    pub safe_mode: bool,
}

/// What the metaserver should do this round.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvictionPlan {
    /// Servers to freeze, ordered by address.
    pub convict: Vec<String>,
    /// Servers the detector called failed but that safe mode held back, ordered
    /// by address. These are the ones an operator needs to look at.
    pub held_by_safe_mode: Vec<String>,
    /// Per-location damage, ordered by location.
    pub damage: Vec<LocationDamage>,
    /// Serving servers observed to have restarted in place, ordered by address.
    /// Reported whether or not the policy converts a reboot into a conviction,
    /// so the restart is visible even when it is not acted on.
    #[serde(default)]
    pub rebooted: Vec<String>,
    /// Servers the orphan guard held back, ordered by address: convicting them
    /// would have left a shard with nobody serving it.
    #[serde(default)]
    pub held_by_orphan_guard: Vec<String>,
    /// The shards that would have been left unserved, ordered by id. Non-empty
    /// here is worth an alert even when the guard is off: it means the cluster
    /// is one conviction away from losing a shard entirely.
    #[serde(default)]
    pub orphaned_shards: Vec<ShardId>,
}

impl ConvictionPlan {
    /// The worst severity across every location this round.
    pub fn worst_severity(&self) -> DamageSeverity {
        self.damage
            .iter()
            .map(|entry| entry.severity)
            .max()
            .unwrap_or(DamageSeverity::Normal)
    }
}

/// Pure correlated-failure gate: group `candidates` by location, assess each
/// location's damage, and convict only the failed servers in locations that are
/// not in safe mode. Deterministic; every output vector is sorted.
pub fn plan_conviction(
    candidates: &[ConvictionCandidate],
    policy: ConvictionPolicy,
) -> ConvictionPlan {
    let mut by_location: BTreeMap<&str, Vec<&ConvictionCandidate>> = BTreeMap::new();
    for candidate in candidates {
        by_location
            .entry(candidate.location.as_str())
            .or_default()
            .push(candidate);
    }

    let mut plan = ConvictionPlan::default();
    for (location, members) in by_location {
        let total = members.len();
        // A server is a conviction target when the detector called it failed, or
        // when it restarted in place and the policy treats that as a failure.
        let is_target = |member: &ConvictionCandidate| {
            member.failed || (member.rebooted && policy.convict_on_reboot)
        };
        // A targeted server counts as damage too: it is about to stop serving.
        let abnormal = members
            .iter()
            .filter(|member| member.abnormal || is_target(member))
            .count();
        let severity = assess_severity(total, abnormal, policy);
        let safe_mode = policy.safe_mode_enabled && severity != DamageSeverity::Normal;

        plan.rebooted.extend(
            members
                .iter()
                .filter(|member| member.rebooted && !member.abnormal)
                .map(|member| member.server_addr.clone()),
        );

        let mut failed = members
            .iter()
            .filter(|member| is_target(member) && !member.abnormal)
            .map(|member| member.server_addr.clone())
            .collect::<Vec<_>>();
        failed.sort();
        if safe_mode || !policy.convict_enabled {
            plan.held_by_safe_mode.extend(failed);
        } else {
            plan.convict.extend(failed);
        }

        plan.damage.push(LocationDamage {
            location: location.to_string(),
            severity,
            total_servers: total,
            abnormal_servers: abnormal,
            safe_mode,
        });
    }
    plan.convict.sort();
    plan.held_by_safe_mode.sort();
    plan.rebooted.sort();
    apply_orphan_guard(&mut plan, candidates, policy);
    plan
}

/// Pull back any conviction that would leave a shard with nobody serving it.
///
/// Runs after safe mode, and over the whole round rather than one server at a
/// time: two servers each holding the only two copies of a shard are individually
/// safe to freeze and collectively not. When a shard would be orphaned, *every*
/// candidate serving it is pulled back, which guarantees the shard keeps a holder
/// in a single deterministic pass.
///
/// The orphaned shards are recorded whether or not the guard is enabled. A
/// cluster one conviction away from losing a shard is worth surfacing even when
/// the policy has chosen to convict anyway.
fn apply_orphan_guard(
    plan: &mut ConvictionPlan,
    candidates: &[ConvictionCandidate],
    policy: ConvictionPolicy,
) {
    if plan.convict.is_empty() {
        return;
    }
    let convicting = plan.convict.iter().cloned().collect::<BTreeSet<_>>();

    // Who would still be serving each shard once this round's convictions land.
    // A candidate that is already abnormal is not serving anything either.
    let mut survivors: BTreeMap<ShardId, usize> = BTreeMap::new();
    let mut doomed: BTreeMap<ShardId, Vec<&str>> = BTreeMap::new();
    for candidate in candidates {
        let losing = candidate.abnormal || convicting.contains(&candidate.server_addr);
        for shard_id in &candidate.serving_shards {
            if losing {
                if convicting.contains(&candidate.server_addr) {
                    doomed
                        .entry(*shard_id)
                        .or_default()
                        .push(candidate.server_addr.as_str());
                }
            } else {
                *survivors.entry(*shard_id).or_default() += 1;
            }
        }
    }

    let mut orphaned = Vec::new();
    let mut pull_back = BTreeSet::new();
    for (shard_id, holders) in doomed {
        if survivors.get(&shard_id).copied().unwrap_or_default() > 0 {
            continue;
        }
        orphaned.push(shard_id);
        for holder in holders {
            pull_back.insert(holder.to_string());
        }
