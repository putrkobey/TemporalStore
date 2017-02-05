#!/usr/bin/env bash
# Install the MatrixArk / TemporalStore memory integration for the OpenAI Codex CLI.
#
# Merges an [mcp_servers.matrixark] block and a `notify` handler into ~/.codex/config.toml,
# idempotently (delimited by markers). Re-running updates the block in place.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# repo root = integrations/codex-plugin/../..
DEFAULT_HOME="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MATRIXARK_HOME="${MATRIXARK_HOME:-${DEFAULT_HOME}}"
CODEX_CONFIG="${CODEX_CONFIG:-${HOME}/.codex/config.toml}"
BEGIN="# >>> matrixark-memory (managed by install-codex.sh) >>>"
END="# <<< matrixark-memory <<<"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --matrixark-home) MATRIXARK_HOME="$2"; shift 2 ;;
    --config) CODEX_CONFIG="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: install-codex.sh [--matrixark-home PATH] [--config ~/.codex/config.toml]"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
