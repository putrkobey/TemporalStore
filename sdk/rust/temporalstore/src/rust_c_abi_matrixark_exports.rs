// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::{c_char, c_int};

use crate::rust_c_abi_helpers::{client_from_handle, cstr_arg, run_c_abi, run_c_abi_string};
use crate::Error;

#[no_mangle]
pub unsafe extern "C" fn temporalstore_rust_matrixark_batch_append_records_json(
    handle: *mut std::ffi::c_void,
    entries_json: *const c_char,
    count_key: *const c_char,
    count_value: *const c_char,
    error: *mut *mut c_char,
) -> c_int {
    run_c_abi(error, || {
        let raw = cstr_arg(entries_json, "entries_json")?;
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&raw).map_err(|err| Error {
            code: 1,
            message: format!("invalid entries json: {err}"),
        })?;
        let entries_owned: Vec<(String, String, String)> = parsed
            .iter()
            .filter_map(|entry| {
                Some((
                    entry.get("key")?.as_str()?.to_string(),
                    entry.get("field")?.as_str()?.to_string(),
                    entry.get("value")?.as_str()?.to_string(),
                ))
