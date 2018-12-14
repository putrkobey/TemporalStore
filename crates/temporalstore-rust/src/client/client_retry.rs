// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::thread;
use std::time::Duration;

use crate::types::Status;

use super::{ClientRetryDecision, ReplicaReadPolicy, TableOptions};

pub(super) fn retry_attempts_for(options: &TableOptions, write: bool) -> usize {
    let retries = if write {
        options.max_write_retries
    } else {
        options.max_read_retries
    };
    retries.saturating_add(1)
}

pub(super) fn replica_read_policy_from_meta(policy: &str) -> ReplicaReadPolicy {
    match policy {
        "first_replica" => ReplicaReadPolicy::FirstReplica,
        "round_robin_replica" => ReplicaReadPolicy::RoundRobinReplica,
        _ => ReplicaReadPolicy::PinPrimary,
    }
}

pub(super) fn sleep_before_retry(options: &TableOptions, attempt: usize) {
    if options.retry_backoff_ms == 0 {
        return;
    }
    let multiplier = u64::try_from(attempt.saturating_add(1)).unwrap_or(u64::MAX);
    let sleep_ms = options.retry_backoff_ms.saturating_mul(multiplier);
    thread::sleep(Duration::from_millis(sleep_ms));
