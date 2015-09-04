// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};

use crate::{
    temporalstore_free_string, CFeaturePointArray, COptions, Error, FeaturePoint, Options, Result,
};

pub(crate) struct CStringOptions {
    metaserver_addr: CString,
    metaserver_consul: CString,
    namespace_name: CString,
    table_name: CString,
    idc: CString,
    host: CString,
    psm: CString,
    log_dir: CString,
}

impl CStringOptions {
    pub(crate) fn new(options: &Options) -> Result<Self> {
        Ok(Self {
            metaserver_addr: cstring(&options.metaserver_addr)?,
            metaserver_consul: cstring(&options.metaserver_consul)?,
            namespace_name: cstring(&options.namespace_name)?,
            table_name: cstring(&options.table_name)?,
            idc: cstring(&options.idc)?,
            host: cstring(&options.host)?,
            psm: cstring(&options.psm)?,
            log_dir: cstring(&options.log_dir)?,
        })
    }

    pub(crate) fn apply(&self, c_options: &mut COptions, options: &Options) {
        c_options.metaserver_addr = self.metaserver_addr.as_ptr();
        c_options.metaserver_consul = self.metaserver_consul.as_ptr();
        c_options.namespace_name = self.namespace_name.as_ptr();
        c_options.table_name = self.table_name.as_ptr();
