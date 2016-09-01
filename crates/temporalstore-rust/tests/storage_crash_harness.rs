// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::process::Command;

use serde::Deserialize;
use temporalstore_rust::StorageRecoveryReport;

#[derive(Debug, Deserialize)]
struct CrashHarnessSummary {
    before_value: Option<String>,
    after_value: Option<String>,
    recovery: StorageRecoveryReport,
}

#[test]
fn storage_crash_harness_recovers_after_abrupt_process_abort() {
    let bin = env!("CARGO_BIN_EXE_storage_crash_harness");
    let dir = tempfile::tempdir().unwrap();

    let aborted = Command::new(bin)
        .args([
            "--root",
            dir.path().to_str().unwrap(),
            "--mode",
            "write-abort",
        ])
        .output()
        .expect("write-abort harness should run");
    assert!(
        !aborted.status.success(),
        "write-abort should terminate the process abruptly"
    );

    let recovered = Command::new(bin)
        .args(["--root", dir.path().to_str().unwrap(), "--mode", "recover"])
        .output()
        .expect("recover harness should run");
    assert!(
        recovered.status.success(),
        "recover failed: stdout={} stderr={}",
        String::from_utf8_lossy(&recovered.stdout),
        String::from_utf8_lossy(&recovered.stderr)
    );

    let summary: CrashHarnessSummary =
        serde_json::from_slice(&recovered.stdout).expect("recover output should be JSON");
    assert_eq!(summary.before_value.as_deref(), Some("before-value"));
    assert_eq!(summary.after_value.as_deref(), Some("after-value"));
    assert_eq!(summary.recovery.wal_records, 2);
    assert_eq!(summary.recovery.index_log_records, 2);
    assert_eq!(summary.recovery.active_block_slab_ids, vec![0, 1]);
    assert_eq!(summary.recovery.live_block_slab_ids, vec![0, 1]);
    assert_eq!(summary.recovery.total_block_refs, 2);
    assert_eq!(summary.recovery.readable_block_refs, 2);
    assert!(summary.recovery.all_live_blocks_readable);
    assert_eq!(summary.recovery.slab_descriptors.len(), 2);
    assert_eq!(summary.recovery.block_slab_reports.len(), 2);
    assert!(summary
        .recovery
        .block_slab_reports
        .iter()
        .all(|report| report.first_error.is_none()));
}

#[test]
fn storage_crash_harness_reports_corrupt_block_after_process_abort() {
    let bin = env!("CARGO_BIN_EXE_storage_crash_harness");
    let dir = tempfile::tempdir().unwrap();

    let aborted = Command::new(bin)
        .args([
            "--root",
            dir.path().to_str().unwrap(),
            "--mode",
            "write-abort",
        ])
        .output()
        .expect("write-abort harness should run");
    assert!(
        !aborted.status.success(),
        "write-abort should terminate the process abruptly"
