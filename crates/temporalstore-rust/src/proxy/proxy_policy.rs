// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::atomic::{AtomicU64, Ordering};

use crate::client::key_is_dropped_by_percent;
use crate::types::{Command, Status};

use super::commands::{proxy_command_is_write, proxy_command_routing_key};
use super::{ProxyOptions, ProxyServingMode};

pub(super) fn proxy_policy_rejection(
    options: &ProxyOptions,
    commands: &[Command],
) -> Option<Status> {
    let has_write = commands.iter().any(proxy_command_is_write);
    if let Some(status) = proxy_serving_rejection(options, has_write) {
        return Some(status);
    }
    let drop_percent = options.drop_percent.min(100);
    // A full drain has to mean every request. The check below can only judge commands that
    // HAVE a routing key and `filter_map` silently discards the ones that do not -- the
    // resource-blob upload path, sequence batch queries, node-embedding reads. A proxy an
    // operator had drained to zero served those in full while its readiness probe reported
    // it as rejecting everything.
    //
    // Below 100 the leak stands on purpose: shedding is per key by construction, and a
    // command with nothing to hash has no share to shed. That is the same boundary the
    // readiness probe draws between a drain and load management.
    if drop_percent >= 100 {
        return Some(proxy_dropped_status());
    }
    if drop_percent > 0
        && commands
            .iter()
            .filter_map(proxy_command_routing_key)
            .any(|key| key_is_dropped_by_percent(&key, drop_percent))
    {
        return Some(proxy_dropped_status());
    }
    None
}

/// Serving-mode half of the policy, split out so request paths that carry no
/// `Command` -- the high-level `/context/*` routes the context gateway uses --
/// are governed by the same drain and write-disable switches as command
/// traffic.
pub(super) fn proxy_serving_rejection(options: &ProxyOptions, has_write: bool) -> Option<Status> {
    if matches!(options.serving_mode, ProxyServingMode::NotServing) {
        return Some(Status::error("proxy_not_serving", "proxy is not serving"));
