#!/usr/bin/env bash
# Single-node TemporalStore supervisor.
#
# Runs the Rust metaserver + datanode together in one container and keeps the
# container alive for as long as both processes stay up. This is the simplest
# way to get a working local TemporalStore for development; it is not a
# production HA topology.
set -euo pipefail

DATA_DIR="${TS_DATA_DIR:-/var/lib/temporalstore}"
META_PORT="${TS_META_PORT:-17101}"
DATA_PORT="${TS_DATA_PORT:-17102}"

# Bind on all interfaces so the ports are reachable from the host and from other
# containers. The two processes talk to each other over localhost *inside* the
# container, so the advertised/meta addresses stay on 127.0.0.1.
export TS_META_BIND_ADDR="0.0.0.0:${META_PORT}"
export TS_META_ADDR="127.0.0.1:${META_PORT}"
export TS_SERVER_BIND_ADDR="0.0.0.0:${DATA_PORT}"
export TS_SERVER_ADVERTISE_ADDR="127.0.0.1:${DATA_PORT}"
export TS_SHARD_ID="${TS_SHARD_ID:-1}"
export TS_CACHE_DIR="${DATA_DIR}/cache"
# The canonical directory, unless the previous one is the one that is already there: a container
# whose volume was created before the rename keeps serving the blocks it holds instead of coming up
# on an empty directory beside them.
if [ -d "${DATA_DIR}/blocks" ] && [ ! -d "${DATA_DIR}/blocks" ]; then
  export TS_BLOCK_STORE_DIR="${DATA_DIR}/blocks"
else
  export TS_BLOCK_STORE_DIR="${DATA_DIR}/blocks"
