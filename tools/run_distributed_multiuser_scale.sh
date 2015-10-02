#!/usr/bin/env bash
# Distributed multi-user scale validation with forced eviction + cold-read promotion.
# Several users, large corpus each, partitioned namespace/table -> shard -> datanode, with a
# small per-datanode memory cache so writes spill memory->disk->block-store and reads promote back.
# Exits non-zero if any iteration fails correctness / partition-isolation / eviction / promotion.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DATANODES="${TS_MU_DATANODES:-8}"
SHARDS="${TS_MU_SHARDS:-32}"
USERS="${TS_MU_USERS:-32}"
STRINGS="${TS_MU_STRINGS_PER_USER:-600}"
FEATURES="${TS_MU_FEATURES_PER_USER:-300}"
