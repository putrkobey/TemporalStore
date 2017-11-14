#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_FLAG=("--release")
CARGO_PREFIX=()

if [[ "${TS_MORE_NODES_PROFILE:-release}" == "debug" ]]; then
  PROFILE_FLAG=()
fi

if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
  CARGO_PREFIX=(env "CARGO_TARGET_DIR=${CARGO_TARGET_DIR}")
fi

cd "$ROOT"

EXTRA_ARGS=()
if [[ -n "${TS_MORE_NODES_SHARED_STORE_ROOT:-}" ]]; then
  EXTRA_ARGS+=(--shared-store-root "${TS_MORE_NODES_SHARED_STORE_ROOT}")
fi
