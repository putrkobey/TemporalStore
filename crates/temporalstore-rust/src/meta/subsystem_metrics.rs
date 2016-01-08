// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Prometheus metrics for the metaserver's background subsystems.
//!
//! Conviction, shard-divergence reconciliation, retention, freeze aging and
//! rebalancing all run on background loops and, until now, reported what they
//! did only through tracing. That leaves an operator scraping logs to answer
//! the questions they will actually ask during an incident: is the fleet being
//! convicted right now, is any location in safe mode, how many shards is the
//! metaserver routing to nodes that do not serve them, is retention keeping up.
//!
//! [`SubsystemMetrics`] is the recorder those loops write their round outcomes
//! into, and [`SubsystemMetrics::prometheus`] renders them onto the metaserver's
//! existing `/metrics` surface. It holds two kinds of series:
//!
//! * **Counters** that only ever climb — how many resources have been convicted,
//!   purged, aged out or reassigned since this metaserver started. These answer
//!   "is this happening, and how often".
//! * **Gauges** describing the most recent round — per-location damage severity,
//!   how many shards are currently diverged, whether a detector is paused. These
//!   answer "what is true right now", and are replaced wholesale each round so a
//!   location that recovers stops reporting damage rather than sticking at its
//!   worst value.
//!
//! The recorder is cheap to clone (one `Arc`) and every method takes `&self`, so
//! the loops write to it without threading a handle through their signatures.
//!
//! One deliberate cardinality note: damage is labelled by location, which
//! matches the reference's per-tag emission. That is bounded by the number of
//! distinct locations in the cluster, but with hierarchical locations a
//! deployment labelling every rack separately will get one series per rack.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::*;

/// Which tier a conviction series describes.
pub const TIER_SERVER: &str = "server";
/// Which tier a conviction series describes.
pub const TIER_PROXY: &str = "proxy";

#[derive(Debug, Default)]
struct SubsystemMetricsState {
    // --- counters ---
    convicted_total: BTreeMap<String, u64>,
    held_total: BTreeMap<(String, String), u64>,
    reboots_detected_total: u64,
    divergences_total: u64,
    /// Topology queries answered with an error rather than a topology.
    topology_query_failed_total: u64,
    /// Recorded mutations that would not apply. During replay this means the
    /// log and the state have parted company.
    mutation_apply_failed_total: u64,
    /// How long topology queries took, in microseconds.
    ///
    /// Counts per bucket, not cumulative -- the rendering adds them up, because
    /// that is the shape Prometheus wants and this is the shape that is cheap to
    /// record.
    topology_latency_us: [u64; TOPOLOGY_LATENCY_BUCKETS_US.len()],
    topology_latency_over: u64,
    topology_latency_sum_us: u64,
    topology_latency_count: u64,
    /// Bytes of topology handed out, as encoded on the wire.
    topology_query_bytes_total: u64,
    /// Shards a topology answer placed on fewer servers than their table asks
    /// for, counted as the answers go out, and how many the last answer was
    /// short of.
    placement_short_total: u64,
    placement_short_now: u64,
    reassigned_total: BTreeMap<String, u64>,
    purged_total: BTreeMap<String, u64>,
    aged_total: BTreeMap<String, u64>,
    rounds_total: BTreeMap<String, u64>,

    // --- last-round gauges ---
    damage: BTreeMap<String, Vec<LocationDamage>>,
    detector_paused: BTreeMap<String, bool>,
    diverged_now: u64,
    settling_now: u64,
    divergence_rate_limited: u64,
    retention_blocked_servers: u64,
    retention_capped: u64,
    freeze_aging_capped: u64,
    divergence_skipped: BTreeMap<String, u64>,
    /// Proxies attached to, or released from, a group by calibration.
    ///
    /// Kept apart from `reassigned_total`, which is rendered as
    /// `shards_reassigned_total` and documented as shard ownership changes: a
    /// proxy joining a group is not a shard moving, and counting it there
    /// inflates the number an operator watches for rebalance churn.
    proxy_attachments_total: BTreeMap<String, u64>,
    calibration_shortfall_groups: u64,
    calibration_shortfall_proxies: u64,
    calibration_capped: u64,
}

/// Shared recorder for background-subsystem outcomes.
#[derive(Debug, Clone, Default)]
pub struct SubsystemMetrics {
    inner: Arc<Mutex<SubsystemMetricsState>>,
}

/// Where the topology-query latency histogram puts things, in microseconds.
///
/// An answer served from the metadata already in memory lands in the first few;
/// the wide ones at the top are there so an answer that waited on the write lock
/// is visible as itself rather than lost in an overflow bucket.
const TOPOLOGY_LATENCY_BUCKETS_US: [u64; 9] =
    [50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 50_000];

impl SubsystemMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a topology query that was refused rather than answered.
    pub fn record_topology_query_failed(&self) {
        self.with(|state| state.topology_query_failed_total += 1);
    }

    /// Record a recorded mutation that would not apply.
    pub fn record_mutation_apply_failed(&self) {
        self.with(|state| state.mutation_apply_failed_total += 1);
    }

    fn with<R>(&self, f: impl FnOnce(&mut SubsystemMetricsState) -> R) -> R {
        // A poisoned metrics lock must never take the metaserver down with it:
        // observability is not worth an outage, so recover the guard and carry on.
        let mut state = match self.inner.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        f(&mut state)
    }

    /// Record one adaptive conviction round for `tier`.
    pub fn record_conviction(&self, tier: &str, report: &AdaptiveConvictionReport) {
        self.with(|state| {
            *state.rounds_total.entry(tier.to_string()).or_default() += 1;
            let convicted = (report.frozen_servers.len() + report.frozen_proxies.len()) as u64;
            *state.convicted_total.entry(tier.to_string()).or_default() += convicted;
            *state
                .held_total
                .entry((tier.to_string(), "safe_mode".to_string()))
                .or_default() += report.held_by_safe_mode.len() as u64;
            state.reboots_detected_total += report.rebooted.len() as u64;
            state
                .damage
                .insert(tier.to_string(), report.damage.clone());
            state
                .detector_paused
                .insert(tier.to_string(), report.detector_paused);
        });
    }

    /// Record one shard-divergence reconciliation round.
    /// Record how long one topology query took.
    pub fn record_topology_latency(&self, elapsed_us: u64) {
        self.with(|state| {
            state.topology_latency_count += 1;
            state.topology_latency_sum_us += elapsed_us;
            match TOPOLOGY_LATENCY_BUCKETS_US
                .iter()
                .position(|bound| elapsed_us <= *bound)
            {
                Some(index) => state.topology_latency_us[index] += 1,
                None => state.topology_latency_over += 1,
            }
        });
    }

    /// Record the encoded size of one topology answer.
    pub fn record_topology_bytes(&self, bytes: usize) {
        self.with(|state| state.topology_query_bytes_total += bytes as u64);
    }

    /// Record what one topology answer could not place.
    ///
    /// `short` counts the shards in that answer which are serving but hold
    /// fewer replicas than their table asks for. Recorded where the answer is
    /// built, because placement is worked out per request and there is no
    /// round anywhere that would otherwise notice.
    pub fn record_placement(&self, short: usize) {
        self.with(|state| {
            state.placement_short_total += short as u64;
            state.placement_short_now = short as u64;
        });
    }

    pub fn record_divergence(&self, report: &ShardCheckReport) {
        self.with(|state| {
            *state
                .rounds_total
                .entry("divergence".to_string())
                .or_default() += 1;
            state.divergences_total += report.diverged.len() as u64;
            state.diverged_now = report.diverged.len() as u64;
            state.settling_now = report.settling.len() as u64;
            state.divergence_rate_limited = report.rate_limited as u64;
            // Servers the check declined to look at. Without these, a reported
            // zero means either "nothing diverged" or "the servers that mattered
            // were never examined", and an operator cannot tell which. A server
            // that has never reported shard states is skipped outright.
            for (reason, servers) in [
                ("reboot_grace", &report.skipped_in_reboot_grace),
                ("no_shard_reports", &report.skipped_without_shard_reports),
            ] {
                state
                    .divergence_skipped
                    .insert(reason.to_string(), servers.len() as u64);
            }
        });
    }

    /// Record one retention round.
    /// Record one proxy calibration round.
    ///
    /// Calibration was the one background subsystem reporting nothing. Its
    /// attach and detach both go through `set_proxy_group`, so they land in the
    /// change history -- but a group left short of its target existed only in
    /// the returned plan, which the loop dropped. `ProxyGroupShortfall` says of
    /// itself that no available proxies "is an operator problem rather than a
    /// metaserver one", and nothing was telling the operator.
    /// Record one calibration round: `plan` is what it set out to do,
    /// `applied` is what it actually changed.
    ///
    /// They match only when the round ran to the end. Calibration returns on
    /// the first `set_proxy_group` that is refused, leaving the rest of the
    /// plan untouched, and counting the plan there reports attachments that
    /// were never made. The shortfall and the cap still come from the plan:
    /// what a round wanted, and what it held back, are facts about planning it.
    pub fn record_calibration(
        &self,
        plan: &ProxyCalibrationPlan,
        applied: &ProxyCalibrationPlan,
    ) {
        self.with(|state| {
            *state
                .rounds_total
                .entry("proxy_calibration".to_string())
                .or_default() += 1;
            *state
                .proxy_attachments_total
                .entry("attach".to_string())
                .or_default() += applied.attach.len() as u64;
            *state
                .proxy_attachments_total
                .entry("detach".to_string())
                .or_default() += applied.detach.len() as u64;
            state.calibration_shortfall_groups = plan.shortfalls.len() as u64;
            // What each group is still short once this round's attaches land.
            // `attached` is the count before the round and `available` is what
            // the round can attach, so leaving `available` out overstates every
            // shortfall by exactly the proxies about to be added.
            state.calibration_shortfall_proxies = plan
                .shortfalls
                .iter()
                .map(|short| {
                    short
                        .wanted
                        .saturating_sub(short.attached)
                        .saturating_sub(short.available)
                })
                .sum();
            state.calibration_capped = plan.capped as u64;
        });
    }

    pub fn record_retention(&self, plan: &MetaRetentionPlan) {
        self.with(|state| {
            *state
                .rounds_total
                .entry("retention".to_string())
                .or_default() += 1;
            for (kind, count) in [
                ("server", plan.servers.len()),
                ("proxy", plan.proxies.len()),
                ("table", plan.tables.len()),
                ("shard", plan.shards.len()),
            ] {
                *state.purged_total.entry(kind.to_string()).or_default() += count as u64;
            }
            state.retention_blocked_servers = plan.blocked_servers.len() as u64;
            state.retention_capped = plan.capped as u64;
        });
    }

    /// Record one freeze-aging round.
    /// Record one aging round: `plan` is what it set out to do, `applied` is
    /// what it actually dropped.
    ///
    /// They are the same only when the round ran to the end. A drop that is
    /// refused returns from the round with the rest of the plan untouched, and
    /// counting the plan there reports resources as aged into the dropped state
    /// while they are still sitting in the metadata, frozen. The cap still comes
    /// from the plan: what a round held back is a fact about planning it.
    pub fn record_freeze_aging(&self, plan: &FreezeAgingPlan, applied: &FreezeAgingPlan) {
        self.with(|state| {
            *state
                .rounds_total
                .entry("freeze_aging".to_string())
                .or_default() += 1;
            for (kind, count) in [
                ("server", applied.servers.len()),
                ("proxy", applied.proxies.len()),
                ("table", applied.tables.len()),
            ] {
                *state.aged_total.entry(kind.to_string()).or_default() += count as u64;
            }
            // Retention reports the work its per-round cap held back; freeze
            // aging has the same field and was not reporting it, so a round
            // doing less than it wanted looked like a round with less to do.
            state.freeze_aging_capped = plan.capped as u64;
        });
    }

    /// Record one shard changing owner, labelled by why it moved.
    pub fn record_reassignment(&self, reason: &str) {
        self.with(|state| {
            *state.reassigned_total.entry(reason.to_string()).or_default() += 1;
        });
    }

    /// Render every series in the Prometheus text format.
    pub fn prometheus(&self) -> String {
        self.with(|state| render(state))
    }
}

fn push(out: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str("=\"");
            // Label values come from locations and reasons, which are operator
            // supplied; escape so a stray quote cannot corrupt the exposition.
            for ch in value.chars() {
                match ch {
                    '\\' => out.push_str("\\\\"),
                    '"' => out.push_str("\\\""),
                    '\n' => out.push_str("\\n"),
                    _ => out.push(ch),
                }
            }
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn severity_value(severity: DamageSeverity) -> u64 {
    match severity {
        DamageSeverity::Normal => 0,
        DamageSeverity::Warning => 1,
        DamageSeverity::Critical => 2,
    }
}

fn render(state: &SubsystemMetricsState) -> String {
    let mut out = String::new();

    out.push_str(
        "# HELP temporalstore_meta_convicted_total Resources frozen by the failure detector.\n",
    );
    out.push_str("# TYPE temporalstore_meta_convicted_total counter\n");
    for (tier, value) in &state.convicted_total {
        push(
            &mut out,
            "temporalstore_meta_convicted_total",
            &[("tier", tier)],
            *value,
        );
    }

    out.push_str("# HELP temporalstore_meta_conviction_held_total Convictions held back by a guard rather than acted on.\n");
    out.push_str("# TYPE temporalstore_meta_conviction_held_total counter\n");
    for ((tier, guard), value) in &state.held_total {
        push(
            &mut out,
            "temporalstore_meta_conviction_held_total",
            &[("tier", tier), ("guard", guard)],
            *value,
        );
    }

    out.push_str(
        "# HELP temporalstore_meta_detector_rounds_total Background subsystem rounds completed.\n",
    );
    out.push_str("# TYPE temporalstore_meta_detector_rounds_total counter\n");
    for (subsystem, value) in &state.rounds_total {
        push(
            &mut out,
            "temporalstore_meta_detector_rounds_total",
            &[("subsystem", subsystem)],
            *value,
        );
    }

    out.push_str("# HELP temporalstore_meta_reboots_detected_total Datanodes observed to have restarted in place.\n");
    out.push_str("# TYPE temporalstore_meta_reboots_detected_total counter\n");
    push(
        &mut out,
        "temporalstore_meta_reboots_detected_total",
        &[],
        state.reboots_detected_total,
    );

    out.push_str("# HELP temporalstore_meta_damage_severity Damage severity per location: 0 normal, 1 warning, 2 critical.\n");
    out.push_str("# TYPE temporalstore_meta_damage_severity gauge\n");
    for (tier, damage) in &state.damage {
        for entry in damage {
            push(
                &mut out,
                "temporalstore_meta_damage_severity",
                &[("tier", tier), ("location", &entry.location)],
                severity_value(entry.severity),
            );
        }
    }

    out.push_str(
        "# HELP temporalstore_meta_abnormal_resources Resources not serving, per location.\n",
    );
    out.push_str("# TYPE temporalstore_meta_abnormal_resources gauge\n");
    for (tier, damage) in &state.damage {
        for entry in damage {
            push(
                &mut out,
                "temporalstore_meta_abnormal_resources",
                &[("tier", tier), ("location", &entry.location)],
                entry.abnormal_servers as u64,
            );
        }
    }

    out.push_str(
        "# HELP temporalstore_meta_location_safe_mode Whether a location is held in safe mode.\n",
    );
    out.push_str("# TYPE temporalstore_meta_location_safe_mode gauge\n");
    for (tier, damage) in &state.damage {
        for entry in damage {
            push(
                &mut out,
                "temporalstore_meta_location_safe_mode",
                &[("tier", tier), ("location", &entry.location)],
                u64::from(entry.safe_mode),
            );
        }
    }

    out.push_str("# HELP temporalstore_meta_detector_paused Whether a detector suppressed conviction this round.\n");
    out.push_str("# TYPE temporalstore_meta_detector_paused gauge\n");
    for (tier, paused) in &state.detector_paused {
        push(
            &mut out,
            "temporalstore_meta_detector_paused",
            &[("tier", tier)],
            u64::from(*paused),
        );
    }

    out.push_str(
        "# HELP temporalstore_meta_topology_query_bytes_total Bytes of topology handed out.\n",
    );
    out.push_str("# TYPE temporalstore_meta_topology_query_bytes_total counter\n");
    push(
        &mut out,
        "temporalstore_meta_topology_query_bytes_total",
        &[],
        state.topology_query_bytes_total,
    );
    out.push_str(
        "# HELP temporalstore_meta_placement_short_total Shards served with fewer replicas than their table asks for.\n",
    );
    out.push_str("# TYPE temporalstore_meta_placement_short_total counter\n");
    push(
        &mut out,
        "temporalstore_meta_placement_short_total",
        &[],
        state.placement_short_total,
    );
    out.push_str(
        "# HELP temporalstore_meta_placement_short Shards the last topology answer could not fill.\n",
    );
    out.push_str("# TYPE temporalstore_meta_placement_short gauge\n");
    push(
        &mut out,
        "temporalstore_meta_placement_short",
        &[],
