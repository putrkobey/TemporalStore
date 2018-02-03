// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "proxy", serde(rename_all = "snake_case"))]
pub enum FeatureFilterOp {
    Equal = 0,
    NotEqual = 1,
    GreaterThan = 2,
    LessThan = 3,
    GreaterOrEqual = 4,
    LessOrEqual = 5,
