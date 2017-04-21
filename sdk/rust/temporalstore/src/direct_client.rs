// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::c_char;
use std::ptr;

use crate::direct_ffi::*;
use crate::direct_helpers::{check, CStringOptions};
use crate::{Options, Result};

pub struct Client {
    pub(crate) raw: *mut TemporalStoreClientOpaque,
}

unsafe impl Send for Client {}

impl Client {
    pub fn connect(options: Options) -> Result<Self> {
        let strings = CStringOptions::new(&options)?;
        let mut c_options = unsafe {
            let mut value = std::mem::MaybeUninit::<COptions>::zeroed().assume_init();
            temporalstore_options_init(&mut value);
            value
        };
        strings.apply(&mut c_options, &options);
