// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::*;
use crate::direct_helpers::{
    check, cstring, feature_points_from_c_array,
};
use crate::{
    FeatureFilter, FeaturePoint, FeatureWritePolicy, Result,
};

impl Client {
    pub fn add_feature_points(&self, key: &str, points: &[FeaturePoint]) -> Result<()> {
        self.add_feature_points_with_policy(key, points, FeatureWritePolicy::Upsert)
    }

    pub fn add_feature_points_with_policy(
        &self,
        key: &str,
        points: &[FeaturePoint],
        policy: FeatureWritePolicy,
    ) -> Result<()> {
        let key = cstring(key)?;
        let values = points
            .iter()
            .map(|point| cstring(&String::from_utf8_lossy(&point.value)))
            .collect::<Result<Vec<_>>>()?;
        let c_points = points
            .iter()
            .zip(values.iter())
            .map(|(point, value)| CFeaturePoint {
                timestamp: point.timestamp_ms,
                value: value.as_ptr(),
            })
            .collect::<Vec<_>>();
        let mut error: *mut c_char = ptr::null_mut();
        let points_ptr = if c_points.is_empty() {
            ptr::null()
        } else {
            c_points.as_ptr()
        };
        let code = unsafe {
            temporalstore_add_feature_points_with_policy(
                self.raw,
                key.as_ptr(),
                points_ptr,
                c_points.len(),
                policy as c_int,
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn query_feature_points(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
    ) -> Result<Vec<FeaturePoint>> {
        let key = cstring(key)?;
        let mut out = CFeaturePointArray {
