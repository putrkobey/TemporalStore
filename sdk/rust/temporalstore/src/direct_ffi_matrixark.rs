// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::{c_char, c_int};

use crate::direct_ffi_types::{CHashEntry, TemporalStoreClientOpaque};

extern "C" {
    pub(crate) fn temporalstore_matrixark_batch_append_records(
        client: *mut TemporalStoreClientOpaque,
        entries: *const CHashEntry,
        entry_count: usize,
        count_key: *const c_char,
        count_value: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_matrixark_batch_append_records_v2(
        client: *mut TemporalStoreClientOpaque,
        entries: *const CHashEntry,
        entry_count: usize,
        count_key: *const c_char,
        count_value: *const c_char,
