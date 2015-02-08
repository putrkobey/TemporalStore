// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::ffi::CString;
use std::os::raw::{c_char, c_int};

use crate::rust_c_abi_helpers::{
    client_from_handle, cstr_arg, options_from_json, run_c_abi, run_c_abi_string, set_error,
};
use crate::{Client, Result};

#[no_mangle]
pub extern "C" fn temporalstore_rust_free_string(value: *mut c_char) {
    if !value.is_null() {
        unsafe {
            let _ = CString::from_raw(value);
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn temporalstore_rust_connect_json(
    options_json: *const c_char,
    out: *mut *mut std::ffi::c_void,
    error: *mut *mut c_char,
) -> c_int {
    if out.is_null() {
        set_error(error, "out handle is null");
        return 1;
    }
    match (|| -> Result<Client> {
        let raw = cstr_arg(options_json, "options_json")?;
        Client::connect(options_from_json(&raw)?)
    })() {
        Ok(client) => {
            *out = Box::into_raw(Box::new(client)) as *mut std::ffi::c_void;
            0
        }
        Err(err) => {
            set_error(error, err.message);
            1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn temporalstore_rust_close(
    handle: *mut std::ffi::c_void,
    error: *mut *mut c_char,
) -> c_int {
    if handle.is_null() {
