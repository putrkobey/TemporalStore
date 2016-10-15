// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::{c_char, c_int};

use crate::direct_ffi_types::{CHashEntryArray, CStringArray, TemporalStoreClientOpaque};

extern "C" {
    pub(crate) fn temporalstore_put_string(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        value: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_put_string_with_ttl(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        value: *const c_char,
        ttl_ms: u64,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_get_string(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        value: *mut *mut c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_delete_object(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_expire(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        ttl_ms: u64,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_ttl(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        ttl_ms: *mut u64,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_hset(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        field: *const c_char,
        value: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_hget(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        field: *const c_char,
        value: *mut *mut c_char,
