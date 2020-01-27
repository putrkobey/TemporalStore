// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::sync::atomic::AtomicI64;

use crate::types::SnapshotStatus;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct SnapshotStatusLabels {
    shard_id: String,
    status: StatusLabel,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ShardLabels {
    shard_id: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
enum StatusLabel {
    Success,
    Failure,
}

impl From<SnapshotStatus> for StatusLabel {
    fn from(value: SnapshotStatus) -> Self {
        match value {
            SnapshotStatus::Success => StatusLabel::Success,
            SnapshotStatus::Failure => StatusLabel::Failure,
