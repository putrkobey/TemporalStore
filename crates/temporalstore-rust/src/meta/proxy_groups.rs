// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Proxy capacity: named groups, and the calibration that keeps them filled.
//!
//! A proxy declares its own `namespace` when it registers, and that is the whole
//! of the story today. Nothing else about the routing tier is expressible:
//!
//! * **A namespace cannot say how much proxy capacity it wants.** There is no
//!   target to compare against, so "this namespace should have four proxies in
//!   this datacenter" lives in a deployment tool, or in somebody's head.
//! * **There is no spare pool.** A proxy that is running and healthy but not
//!   claimed by anything is indistinguishable from one that is serving, because
//!   the only thing that assigns work is the proxy's own configuration.
//! * **Nothing refills a namespace that loses proxies.** When the failure
//!   detector freezes two of a namespace's four proxies, the namespace runs at
//!   half capacity until a human notices and reconfigures a replacement, even
//!   when idle proxies are sitting in the same location.
//!
//! A [`ProxyGroupInfo`] is the missing declaration: a named pool that wants
//! `instance_num` proxies for one namespace, within one location. Proxies are
//! attached to a group by the metaserver rather than by their own config, so the
//! assignment is something the cluster owns and can repair.
//!
//! [`plan_proxy_calibration`] is the repair. Each round it compares every
//! group's target against what is actually attached and serving, attaches idle
//! proxies to make up a shortfall, and detaches surplus. It is pure and
//! deterministic — groups are visited in name order and every tie breaks on the
//! proxy address — so the same state always produces the same plan.
//!
//! Location is matched **hierarchically**, so a group placed at `us-east/dc1`
//! draws from any proxy beneath it rather than from one exact rack. An empty
//! location means "anywhere".

use std::collections::BTreeMap;

use super::*;

/// A named pool of proxies serving one namespace within one location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyGroupInfo {
    /// Unique name, and the key proxies are attached by.
    pub group: String,
    /// The namespace proxies in this group serve.
    pub namespace: String,
    /// Where the group draws proxies from. Matched hierarchically, so a
    /// datacenter-level placement accepts any rack beneath it. Empty means
    /// anywhere.
    #[serde(default)]
    pub location: String,
    /// How many serving proxies the group wants.
    pub instance_num: u64,
    /// Bumped whenever the served configuration changes, so an attached proxy
    /// can tell from its heartbeat that it must re-read.
    #[serde(default)]
    pub config_version: u64,
    /// What percentage of traffic attached proxies should shed, 0 to 100.
    ///
    /// The proxy has always implemented this -- it reads the figure from its
    /// heartbeat, applies it, and exports it as a metric -- but the metaserver
    /// sent a hard-coded zero in every response, so the lever could only be
    /// pulled by restarting each proxy with different configuration, during
    /// exactly the incident where restarting proxies is least welcome.
    #[serde(default)]
    pub drop_percent: u8,
    pub state: MetaEntityState,
}

/// What an attached proxy should be serving, as resolved from its group.
pub(super) struct ProxyServedConfig {
    pub changed: bool,
    pub namespace: String,
    pub config_version: u64,
    pub drop_percent: u8,
}

/// Create a group, or update the target and placement of an existing one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutProxyGroupRequest {
    pub group: String,
    pub namespace: String,
    #[serde(default)]
    pub location: String,
    pub instance_num: u64,
    /// Percentage of traffic attached proxies should shed. Omitted means none,
    /// which is what every existing group means.
    #[serde(default)]
    pub drop_percent: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropProxyGroupRequest {
    pub group: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListProxyGroupsResponse {
    pub status: Status,
    pub groups: Vec<ProxyGroupInfo>,
}

/// One proxy joining or leaving a group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyAttachment {
    pub proxy_addr: String,
    /// The group being joined, or the one being left.
    pub group: String,
}

/// A group that could not be filled this round, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyGroupShortfall {
    pub group: String,
    pub wanted: u64,
    pub attached: u64,
    /// Idle proxies eligible for this group at the time of planning. Zero here
    /// with a shortfall means the cluster has no spare capacity in that
    /// location, which is an operator problem rather than a metaserver one.
    pub available: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyCalibrationOptions {
    /// Most attach/detach decisions applied in one round, so a large
    /// reconfiguration lands gradually rather than moving the whole routing tier
    /// at once.
    pub max_changes_per_round: usize,
}

impl Default for ProxyCalibrationOptions {
    fn default() -> Self {
        Self {
            max_changes_per_round: 16,
        }
    }
}

/// What one calibration round should change.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyCalibrationPlan {
    /// Idle proxies to attach, ordered by proxy address.
    pub attach: Vec<ProxyAttachment>,
    /// Proxies to release back to the idle pool, ordered by proxy address.
    pub detach: Vec<ProxyAttachment>,
    /// Groups still short of their target after this round, ordered by name.
    pub shortfalls: Vec<ProxyGroupShortfall>,
    /// Changes the per-round cap held back.
    pub capped: usize,
}

impl ProxyCalibrationPlan {
    pub fn is_empty(&self) -> bool {
        self.attach.is_empty() && self.detach.is_empty()
    }

    pub fn change_count(&self) -> usize {
        self.attach.len() + self.detach.len()
    }
}

/// What one calibration round actually did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyCalibrationReport {
    pub status: Status,
    pub plan: ProxyCalibrationPlan,
}

/// Is this proxy a candidate for attachment right now?
///
/// It must be serving, unattached, and have been heard from — a proxy that has
/// never heartbeated is a registration, not capacity.
fn is_idle_candidate(proxy: &ProxyMetaInfo) -> bool {
    proxy.state == MetaEntityState::Normal
        && proxy.group.is_empty()
        // Counted heartbeats, not the timestamp. Registration stamps `last_heartbeat_ms`, so
        // testing it against zero asked a question every registered proxy answered the same
        // way -- the rule this line exists to enforce was not in force.
        && proxy.heartbeats_total != 0
}

/// Pure planner: bring every group to its declared capacity.
///
/// Detachments are decided before attachments, so a proxy released by a shrunk
/// or dropped group is not also counted as available to fill another group in
/// the same round — that would attach a proxy the caller has not detached yet.
/// It becomes available on the next round instead, which keeps each round's
/// plan independently applicable.
/// Plan a calibration round from records the caller owns.
///
/// Kept so callers holding owned records still have something to call; it
/// borrows from them rather than copying them again. The metaserver itself uses
/// [`plan_proxy_calibration_borrowed`], which never makes the owned copies at
/// all.
pub fn plan_proxy_calibration(
    groups: &[ProxyGroupInfo],
    proxies: &[ProxyMetaInfo],
    options: ProxyCalibrationOptions,
) -> ProxyCalibrationPlan {
    let groups = groups.iter().collect::<Vec<_>>();
    let proxies = proxies.iter().collect::<Vec<_>>();
    plan_proxy_calibration_borrowed(&groups, &proxies, options)
}

/// Plan a calibration round without owning anything.
///
/// A round reads four fields of a proxy's fifteen -- its address, its group,
/// its location and its state -- so copying the record to read them copies ten
/// fields nobody looks at, two of them Strings, for every proxy in the cluster,
/// every round.
pub fn plan_proxy_calibration_borrowed(
    groups: &[&ProxyGroupInfo],
    proxies: &[&ProxyMetaInfo],
    options: ProxyCalibrationOptions,
) -> ProxyCalibrationPlan {
    let mut plan = ProxyCalibrationPlan::default();
    let live_groups = groups
        .iter()
        .filter(|group| group.state == MetaEntityState::Normal)
        .map(|group| (group.group.as_str(), group))
        .collect::<BTreeMap<_, _>>();

    let mut sorted_proxies = proxies.iter().collect::<Vec<_>>();
    sorted_proxies.sort_by(|left, right| left.proxy_addr.cmp(&right.proxy_addr));

    // 1. Release anything attached to a group that is gone, frozen or dropped.
    //    Its capacity no longer exists, so holding the proxy strands it.
    let mut wanted_detach = Vec::new();
    for proxy in &sorted_proxies {
        if proxy.group.is_empty() {
            continue;
        }
        if !live_groups.contains_key(proxy.group.as_str()) {
            wanted_detach.push(ProxyAttachment {
                proxy_addr: proxy.proxy_addr.clone(),
                group: proxy.group.clone(),
            });
        }
    }

    // Everything below is asked once per group, and none of it changes with
    // the group being asked.
    //
    // Each proxy's location was parsed, and allocated, again for every group in
    // the tier. Its group membership was found by walking every proxy again for
    // every group. Both are properties of the proxy.
    let proxy_locations = sorted_proxies
        .iter()
        .map(|proxy| Location::parse(&proxy.location))
        .collect::<Vec<_>>();
    let mut attached_by_group: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for proxy in &sorted_proxies {
        // A frozen proxy is attached but not serving, so it does not count
        // toward the target -- otherwise a group silently runs short every time
        // the failure detector freezes one of its members.
        if proxy.group.is_empty() || proxy.state != MetaEntityState::Normal {
            continue;
        }
        attached_by_group
            .entry(proxy.group.as_str())
            .or_default()
            .push(proxy.proxy_addr.clone());
    }

    // 2. Per group, compare the target against what is actually serving.
    let mut wanted_attach = Vec::new();
    // Which proxies this round has already promised to a group. Asked once per
    // eligible proxy per group, which is why it is a set and not a scan of the
    // attachments planned so far.
    let mut claimed: BTreeSet<String> = BTreeSet::new();
    for (name, group) in &live_groups {
        let mut attached = attached_by_group
            .get(*name)
            .cloned()
            .unwrap_or_default();
        attached.sort();

        let target = group.instance_num as usize;
        if attached.len() > target {
            // Shed the surplus from the end, so the longest-standing members by
            // address ordering are the ones that stay.
            for addr in attached.iter().skip(target).rev() {
                wanted_detach.push(ProxyAttachment {
                    proxy_addr: addr.clone(),
                    group: (*name).to_string(),
                });
            }
            continue;
        }

        let short = target - attached.len();
        if short == 0 {
            continue;
        }
        let pattern = Location::parse(&group.location);
        let eligible = sorted_proxies
            .iter()
            .enumerate()
            .filter(|(_, proxy)| is_idle_candidate(proxy))
            .filter(|(index, _)| proxy_locations[*index].belongs_to(&pattern))
            .filter(|(_, proxy)| !claimed.contains(proxy.proxy_addr.as_str()))
            .map(|(_, proxy)| proxy.proxy_addr.clone())
            .collect::<Vec<_>>();

        for addr in eligible.iter().take(short) {
            claimed.insert(addr.clone());
            wanted_attach.push(ProxyAttachment {
                proxy_addr: addr.clone(),
                group: (*name).to_string(),
            });
        }
        if eligible.len() < short {
            plan.shortfalls.push(ProxyGroupShortfall {
                group: (*name).to_string(),
                wanted: group.instance_num,
                attached: attached.len() as u64,
                available: eligible.len() as u64,
            });
        }
    }

    wanted_detach.sort_by(|left, right| left.proxy_addr.cmp(&right.proxy_addr));
    wanted_detach.dedup_by(|left, right| left.proxy_addr == right.proxy_addr);
    wanted_attach.sort_by(|left, right| left.proxy_addr.cmp(&right.proxy_addr));

    // 3. Spend the round's budget, detachments first: releasing a stranded proxy
    //    is always safe, while an attachment commits capacity.
    let total = wanted_detach.len() + wanted_attach.len();
    let mut budget = options.max_changes_per_round;
    for item in wanted_detach {
        if budget == 0 {
            break;
        }
        budget -= 1;
        plan.detach.push(item);
    }
    for item in wanted_attach {
        if budget == 0 {
            break;
        }
        budget -= 1;
        plan.attach.push(item);
    }
    plan.capped = total.saturating_sub(plan.change_count());
    plan
}

impl SingleNodeMeta {
    /// Create a proxy group, or update an existing one's target and placement.
    ///
    /// Changing what a group serves bumps `config_version`, which is how an
    /// attached proxy learns from its next heartbeat that it must re-read.
    pub fn put_proxy_group(&self, request: PutProxyGroupRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        // Through the same judgement the propose path uses, so the two
        // cannot drift apart again.
        if let Some(status) =
