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
