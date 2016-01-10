// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fmt;

#[derive(Debug)]
pub struct Error {
    pub code: i32,
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

mod control_state_types;
mod feature_types;
#[path = "value_type_exports.rs"]
mod types;
pub use types::{
    ControlStateFolType, ControlStateHType, ControlStatePrecision, ControlStateWindow,
    ControlStateWindowUnit, FeatureFilter, FeatureFilterOp, FeaturePoint, FeatureWritePolicy, SequenceFeatureRow,
};

#[path = "client_options.rs"]
mod options;
pub use options::Options;

#[cfg(feature = "direct")]
mod direct_client;
#[cfg(feature = "direct")]
mod direct_client_matrixark_append;
#[cfg(feature = "direct")]
mod direct_client_matrixark_retrieve;
#[cfg(feature = "direct")]
mod direct_control_state;
#[cfg(feature = "direct")]
mod direct_features;
#[cfg(feature = "direct")]
mod direct_ffi;
#[cfg(feature = "direct")]
mod direct_ffi_capabilities;
#[cfg(feature = "direct")]
mod direct_ffi_core;
#[cfg(feature = "direct")]
mod direct_ffi_kv;
#[cfg(feature = "direct")]
mod direct_ffi_matrixark;
#[cfg(feature = "direct")]
mod direct_ffi_types;
#[cfg(feature = "direct")]
mod direct_hash;
