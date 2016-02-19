// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum ControlStatePrecision {
    OneSecond = 0,
    FiveSeconds = 1,
    TenSeconds = 2,
    OneMinute = 3,
    FiveMinutes = 4,
    TenMinutes = 5,
    OneHour = 6,
    OneDay = 7,
    OneMonth = 8,
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum ControlStateWindowUnit {
