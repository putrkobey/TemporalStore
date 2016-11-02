// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::c_char;
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::*;
use crate::direct_helpers::{check, cstring};
use crate::{ControlStatePrecision, ControlStateWindow, Result};

impl Client {
    pub fn control_state_increment(
        &self,
        key: &str,
        amount: i64,
