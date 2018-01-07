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
        }
    }
}

#[derive(Clone)]
pub struct SnapshotMetrics {
    create_total: Family<SnapshotStatusLabels, Counter>,
    upload_total: Family<SnapshotStatusLabels, Counter>,
    download_total: Family<SnapshotStatusLabels, Counter>,
    restore_total: Family<SnapshotStatusLabels, Counter>,
    snapshot_bytes: Family<ShardLabels, Gauge<i64, AtomicI64>>,
    upload_seconds: Family<ShardLabels, Gauge<i64, AtomicI64>>,
    download_seconds: Family<ShardLabels, Gauge<i64, AtomicI64>>,
    last_success_log_index: Family<ShardLabels, Gauge<i64, AtomicI64>>,
}

impl SnapshotMetrics {
    pub fn register(registry: &mut Registry) -> Self {
        let create_total = Family::<SnapshotStatusLabels, Counter>::default();
        let upload_total = Family::<SnapshotStatusLabels, Counter>::default();
        let download_total = Family::<SnapshotStatusLabels, Counter>::default();
        let restore_total = Family::<SnapshotStatusLabels, Counter>::default();
        let snapshot_bytes = Family::<ShardLabels, Gauge<i64, AtomicI64>>::default();
        let upload_seconds = Family::<ShardLabels, Gauge<i64, AtomicI64>>::default();
        let download_seconds = Family::<ShardLabels, Gauge<i64, AtomicI64>>::default();
        let last_success_log_index = Family::<ShardLabels, Gauge<i64, AtomicI64>>::default();

        registry.register(
            "temporalstore_snapshot_create",
            "Total local snapshot creation attempts.",
            create_total.clone(),
        );
        registry.register(
            "temporalstore_snapshot_upload",
