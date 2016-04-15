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
