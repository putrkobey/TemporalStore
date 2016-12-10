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
        policy: FeatureWritePolicy,
    ) -> Result<()> {
        let key = cstring(key)?;
        let c_rows: Vec<CSequenceFeatureRow> = rows
            .iter()
            .map(|row| CSequenceFeatureRow {
                timestamp: row.timestamp,
                gid: row.gid,
                action_type: row.action_type,
                duration: row.duration,
                author_id: row.author_id,
            })
            .collect();
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_add_sequence_feature_rows_with_policy(
                self.raw,
                key.as_ptr(),
                c_rows.as_ptr(),
                c_rows.len(),
                policy as c_int,
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn query_sequence_feature_rows(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: &[FeatureFilter],
    ) -> Result<Vec<SequenceFeatureRow>> {
        let key = cstring(key)?;
        let c_fields: Vec<CString> = filters
            .iter()
            .map(|filter| cstring(&filter.field))
            .collect::<Result<Vec<_>>>()?;
        let c_filters: Vec<CFeatureFilter> = filters
            .iter()
            .zip(c_fields.iter())
