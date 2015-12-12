// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(super) fn push_metric(out: &mut String, name: &str, labels: &[(&str, String)], value: u64) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
