#!/usr/bin/env bash
# =============================================================================
# deploy_onebox.sh - one node, one process, memory + local disk (EBS).
#
# The smallest thing that is a whole TemporalStore: no metaserver, no peers, no
# shared store. The durable copy is this node's own disk, so there is no
# distance for an on-disk cache tier to span and none is opened - the cache is
# memory, and the durable tier is the volume under TS_PROFILE_DATA.
#
# This is the default shape. A datanode started with no configuration at all
# lands here, and this script is that shape written down and checked rather than
# left implicit.
#
# Usage:
#   sudo tools/deploy_onebox.sh
#   TS_PROFILE_DATA=/mnt/ebs/ts TS_SERVER_ADDR=0.0.0.0:17002 tools/deploy_onebox.sh
# =============================================================================
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/deploy_profile_common.sh"
