#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AWS_MODE="${AWS_MODE:-0}"

cd "${ROOT}"

echo "== local snapshot smoke =="
