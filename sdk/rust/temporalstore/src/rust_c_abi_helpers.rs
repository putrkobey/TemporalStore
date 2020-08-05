// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};

use crate::{Client, Error, Options, Result};

fn rust_c_error(message: impl Into<String>) -> *mut c_char {
    CString::new(message.into())
        .unwrap_or_else(|_| CString::new("Rust TemporalStore direct SDK error").unwrap())
        .into_raw()
}

pub(crate) fn rust_c_string(value: impl Into<String>) -> *mut c_char {
    CString::new(value.into())
        .unwrap_or_else(|_| CString::new("").unwrap())
        .into_raw()
}

pub(crate) unsafe fn set_error(error: *mut *mut c_char, message: impl Into<String>) {
    if !error.is_null() {
        *error = rust_c_error(message);
    }
}

pub(crate) unsafe fn client_from_handle<'a>(
    handle: *mut std::ffi::c_void,
) -> Result<&'a mut Client> {
