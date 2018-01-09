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
