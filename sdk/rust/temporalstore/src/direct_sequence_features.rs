// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::*;
use crate::direct_helpers::{check, cstring};
use crate::{FeatureFilter, FeatureWritePolicy, Result, SequenceFeatureRow};

impl Client {
    pub fn add_sequence_feature_rows(&self, key: &str, rows: &[SequenceFeatureRow]) -> Result<()> {
        self.add_sequence_feature_rows_with_policy(key, rows, FeatureWritePolicy::Upsert)
    }

    pub fn add_sequence_feature_rows_with_policy(
        &self,
        key: &str,
        rows: &[SequenceFeatureRow],
