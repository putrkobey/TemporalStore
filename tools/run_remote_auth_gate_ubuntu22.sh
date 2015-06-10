#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-remote-auth-gate-$(date +%Y%m%d_%H%M%S)}"
REMOTE_NAME="${REMOTE_NAME:-origin}"
EXPECTED_REMOTE_REGEX="${EXPECTED_REMOTE_REGEX:-bjmeetsfo/TemporalStore\\.git}"
REMOTE_TIMEOUT_S="${REMOTE_TIMEOUT_S:-30}"
TEXTFILE_DIR="${TEXTFILE_DIR:-${RESULT_DIR}/metrics}"
METRICS_FILE="${METRICS_FILE:-${TEXTFILE_DIR}/temporalstore-remote-auth.prom}"

mkdir -p "${RESULT_DIR}" "${TEXTFILE_DIR}"
SUMMARY="${RESULT_DIR}/summary.txt"
ls_remote_code=1
expected_remote_match=0
visible_refs=0
credential_helper_gh_missing=0

write_metrics() {
  local pass="$1"
  cat > "${METRICS_FILE}" <<METRICS
# HELP temporalstore_remote_auth_gate_pass Whether the non-interactive git remote auth gate passed.
# TYPE temporalstore_remote_auth_gate_pass gauge
temporalstore_remote_auth_gate_pass ${pass}
# HELP temporalstore_remote_auth_ls_remote_exit_code Exit code from git ls-remote with terminal prompts disabled.
# TYPE temporalstore_remote_auth_ls_remote_exit_code gauge
temporalstore_remote_auth_ls_remote_exit_code ${ls_remote_code}
# HELP temporalstore_remote_auth_expected_remote_match Whether the configured remote URL matches the expected account/repository.
# TYPE temporalstore_remote_auth_expected_remote_match gauge
temporalstore_remote_auth_expected_remote_match ${expected_remote_match}
# HELP temporalstore_remote_auth_visible_refs Number of refs returned by git ls-remote.
# TYPE temporalstore_remote_auth_visible_refs gauge
temporalstore_remote_auth_visible_refs ${visible_refs}
# HELP temporalstore_remote_auth_credential_helper_gh_missing Whether git config requires gh auth but gh is unavailable.
# TYPE temporalstore_remote_auth_credential_helper_gh_missing gauge
temporalstore_remote_auth_credential_helper_gh_missing ${credential_helper_gh_missing}
METRICS
}

log() {
  printf '%s\n' "$*" | tee -a "${SUMMARY}"
