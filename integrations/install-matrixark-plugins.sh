#!/usr/bin/env bash
# Install MatrixArk / TemporalStore memory into Claude Code and/or the OpenAI Codex CLI,
# using the modern plugin surfaces:
#   - Claude Code: the marketplace plugin `matrixark-memory@temporalstore`
#   - Codex CLI:   [mcp_servers.matrixark] + notify in ~/.codex/config.toml
#
# Usage:
#   bash integrations/install-matrixark-plugins.sh [--agent claude|codex|both]
#                                                  [--matrixark-home PATH]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
AGENT="both"
MATRIXARK_HOME="${MATRIXARK_HOME:-${REPO_ROOT}}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --agent) AGENT="$2"; shift 2 ;;
    --matrixark-home) MATRIXARK_HOME="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: install-matrixark-plugins.sh [--agent claude|codex|both] [--matrixark-home PATH]"; exit 0 ;;
