// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::{c_char, c_int};

use crate::direct_ffi_types::{
    CFeatureFilter, CFeaturePoint, CFeaturePointArray, CSequenceFeatureRow,
    CSequenceFeatureRowArray, TemporalStoreClientOpaque,
};

extern "C" {
    pub(crate) fn temporalstore_add_feature_points_with_policy(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        points: *const CFeaturePoint,
        count: usize,
        policy: c_int,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_query_feature_points(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        points: *mut CFeaturePointArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_query_feature_points_with_filters(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: *const CFeatureFilter,
        filter_count: usize,
        points: *mut CFeaturePointArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_feature_point_array_free(points: *mut CFeaturePointArray);
    pub(crate) fn temporalstore_add_sequence_feature_rows_with_policy(
