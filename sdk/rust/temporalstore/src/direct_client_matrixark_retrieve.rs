// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::*;
use crate::direct_helpers::{check, cstring};
use crate::direct_matrixark_guard::native_matrixark_c_api_bridge_allowed;
use crate::Result;

impl Client {
