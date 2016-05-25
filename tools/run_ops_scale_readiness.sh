#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_LOCAL_SCALE=false
RUN_DISTRIBUTED_RAFT=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-local-scale)
      RUN_LOCAL_SCALE=true
      shift
      ;;
    --run-distributed-raft)
      RUN_DISTRIBUTED_RAFT=true
      shift
      ;;
    --help|-h)
      cat <<'USAGE'
usage: tools/run_ops_scale_readiness.sh [--run-local-scale] [--run-distributed-raft]

Runs the ops/scale readiness evidence gate. By default it validates that the
production evidence contract, docs, dashboards, harnesses, and unified corpus
are present. Optional flags run the heavier local scale and distributed Raft
process harnesses.
USAGE
      exit 0
      ;;
