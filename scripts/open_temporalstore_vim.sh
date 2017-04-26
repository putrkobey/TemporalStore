#!/usr/bin/env bash
set -euo pipefail

TS_ROOT="/opt/github-services/TemporalStore"
WORKDIR="$TS_ROOT/crates/temporalstore-rust"
MAIN_FILE="$WORKDIR/src/lib.rs"

cd "$TS_ROOT"

if command -v ctags >/dev/null 2>&1 && command -v cscope >/dev/null 2>&1; then
  find crates/temporalstore-rust crates/temporalstore-snapshot \
    -type f \( -name '*.rs' -o -name '*.toml' \) | sort > cscope.files
