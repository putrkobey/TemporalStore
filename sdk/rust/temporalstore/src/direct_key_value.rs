// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::{
    temporalstore_free_string, temporalstore_get_string, temporalstore_put_string,
    temporalstore_put_string_with_ttl,
};
use crate::direct_helpers::{check, cstring};
