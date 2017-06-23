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
    }
    if has_write
        && matches!(
            options.serving_mode,
            ProxyServingMode::Readonly | ProxyServingMode::WriteDisabled
        )
    {
        return Some(Status::error(
            "proxy_write_disabled",
            "proxy is not accepting writes",
        ));
    }
    None
}

/// Deterministic drop for a single routing key, so `/context/*` sheds whole
/// tenants rather than a random slice of one tenant's requests.
pub(super) fn proxy_drop_rejection(options: &ProxyOptions, routing_key: &str) -> Option<Status> {
    let drop_percent = options.drop_percent.min(100);
    if drop_percent > 0 && key_is_dropped_by_percent(routing_key, drop_percent) {
        return Some(proxy_dropped_status());
    }
    None
}

fn proxy_dropped_status() -> Status {
    // Says "for this key" on purpose. The decision comes from a hash of the routing key, so it
    // is the same on every attempt: a caller that reads this as transient and retries will be
    // refused identically for as long as the setting stands. The old wording -- "request
    // dropped" -- invited exactly that retry loop.
    Status::error(
        "proxy_traffic_dropped",
        "request refused for this key by proxy drop_percent; the same key is refused on \
         every attempt while drop_percent stands, so retrying it will not succeed",
    )
}

#[cfg(test)]
mod message_tests {
    use super::*;

    #[test]
    fn the_refusal_a_caller_sees_reads_as_one_sentence() {
        // This goes back over the wire to whoever made the request. A literal
        // wrapped across source lines keeps the indentation of the continuation
        // unless it is escaped, and this one was arriving with ten spaces in
        // the middle of the sentence.
        let status = proxy_dropped_status();
        assert_eq!(status.code, "proxy_traffic_dropped");
        assert!(
            !status.message.contains("  "),
            "the refusal reads with a run of spaces in it: {:?}",
            status.message
        );
    }
}

pub(super) fn proxy_serving_mode_from_meta(value: &str) -> Option<ProxyServingMode> {
    match value.to_ascii_lowercase().replace('-', "_").as_str() {
        "" => None,
        "serving" => Some(ProxyServingMode::Serving),
        "readonly" | "read_only" => Some(ProxyServingMode::Readonly),
        "write_disabled" => Some(ProxyServingMode::WriteDisabled),
        "degraded" => Some(ProxyServingMode::Degraded),
        "not_serving" | "disabled" | "frozen" | "dropped" => Some(ProxyServingMode::NotServing),
        _ => None,
    }
}

pub(super) fn proxy_serving_mode_label(mode: ProxyServingMode) -> &'static str {
    match mode {
        ProxyServingMode::Serving => "serving",
        ProxyServingMode::Readonly => "readonly",
        ProxyServingMode::WriteDisabled => "write_disabled",
        ProxyServingMode::Degraded => "degraded",
        ProxyServingMode::NotServing => "not_serving",
    }
}

/// Rejects a request whose namespace falls outside the account this proxy is
/// scoped to. Enforcement with no configured account fails closed: a proxy that
/// is told to enforce but given nothing to enforce against must not serve
/// everything.
pub(super) fn proxy_account_rejection(options: &ProxyOptions, namespace: &str) -> Option<Status> {
    if !options.enforce_ingestion_account {
        return None;
    }
    if options.ingestion_account.is_empty() {
        return Some(Status::error(
            "proxy_account_not_configured",
            "proxy account enforcement is enabled without an ingestion_account",
        ));
    }
    if namespace != options.ingestion_account {
        return Some(Status::error(
            "proxy_account_denied",
            "namespace is not allowed for this proxy account",
        ));
    }
    None
}
