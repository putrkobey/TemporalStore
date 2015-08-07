// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#[derive(Clone, Debug)]
pub struct Options {
    pub metaserver_addr: String,
    pub namespace_name: String,
    pub table_name: String,
    pub metaserver_consul: String,
    pub idc: String,
    pub host: String,
    pub psm: String,
    pub log_dir: String,
    pub log_level: i32,
    pub io_timeout_ms: i32,
    pub connect_timeout_ms: i32,
    pub request_timeout_ms: i32,
    pub max_read_retries: i32,
    pub max_write_retries: i32,
    pub retry_backoff_ms: i32,
    pub max_feature_points_per_request: i32,
    pub max_feature_query_count: u64,
    pub max_key_bytes: u64,
    pub max_value_bytes: u64,
    pub pin_primary: bool,
}

impl Options {
