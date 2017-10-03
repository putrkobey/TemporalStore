// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(super) fn push_proxy_metric(out: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
