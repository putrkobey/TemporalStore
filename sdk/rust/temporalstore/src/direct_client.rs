// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::c_char;
use std::ptr;

use crate::direct_ffi::*;
use crate::direct_helpers::{check, CStringOptions};
use crate::{Options, Result};

pub struct Client {
    pub(crate) raw: *mut TemporalStoreClientOpaque,
