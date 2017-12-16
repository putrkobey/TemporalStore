// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::{c_char, c_int};

#[repr(C)]
pub(crate) struct TemporalStoreClientOpaque {
    pub(crate) _private: [u8; 0],
}

#[repr(C)]
pub(crate) struct COptions {
    pub(crate) metaserver_addr: *const c_char,
    pub(crate) metaserver_consul: *const c_char,
    pub(crate) namespace_name: *const c_char,
    pub(crate) table_name: *const c_char,
    pub(crate) idc: *const c_char,
    pub(crate) host: *const c_char,
    pub(crate) psm: *const c_char,
    pub(crate) log_dir: *const c_char,
    pub(crate) log_level: c_int,
    pub(crate) io_timeout_ms: c_int,
    pub(crate) connect_timeout_ms: c_int,
    pub(crate) request_timeout_ms: c_int,
    pub(crate) max_read_retries: c_int,
    pub(crate) max_write_retries: c_int,
    pub(crate) retry_backoff_ms: c_int,
    pub(crate) max_feature_points_per_request: c_int,
    pub(crate) max_feature_query_count: u64,
    pub(crate) max_key_bytes: u64,
