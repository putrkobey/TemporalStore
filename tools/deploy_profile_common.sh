#!/usr/bin/env bash
# =============================================================================
# deploy_profile_common.sh - the shared body of the three profile launchers.
#
# TemporalStore ships three deployment profiles, and they differ in exactly two
# things: how many nodes there are, and whether a node has to reach across a
# distance to the authoritative copy.
#
#   one-box         1 node    memory + local disk (EBS)
#   raft            N nodes   memory + local disk (EBS), per node
#   shared-storage  N nodes   memory + local disk + one shared store
#
# One box and raft are the SAME storage shape - both keep the durable copy on
# the node's own disk, so neither wants the on-disk cache tier, which exists to
# span the distance to a shared store. They differ in node count, and that is
# the whole difference. Scaling out defaults to raft; shared storage is opt-in.
#
# Everything below this line is identical across the three profiles on purpose.
# The performance flags are set EXPLICITLY rather than left to their defaults:
# several of them still default off in the engine, and a deployment that is
# "fast when the defaults happen to be right" is not reproducible.
#
# Sourced, never run directly. A profile script sets TS_PROFILE_EXPECT and the
# handful of vars its shape requires, then calls ts_profile_launch.
# =============================================================================
set -euo pipefail

TS_PROFILE_BIN="${TS_PROFILE_BIN:-/opt/temporalstore/bin/matrixark_rust_datanode}"
TS_PROFILE_DATA="${TS_PROFILE_DATA:-/var/lib/temporalstore}"
TS_PROFILE_LOG="${TS_PROFILE_LOG:-${TS_PROFILE_DATA}/node.log}"
TS_PROFILE_WAIT_S="${TS_PROFILE_WAIT_S:-20}"

# --- performance flags: live on every profile --------------------------------
# Value vocabulary, identical to the copy in matrixark_mcp_rust_server.sh,
# matrixark_claude_hook.sh, matrixark_codex_rust_hook.sh and matrixark_backfill_daemon.sh.
# Kept as a copy rather than sourced: these five run as standalone scripts in deployments
# where a missing shared file would be a failure to start, and the function is pure and six
# lines long. test_every_shell_copy_of_the_flag_vocabulary_agrees.py holds them level.
matrixark_flag_on() {  # $1 = value, $2 = default ("1" means on)
  # Whitespace is deleted before the match, not merely case-folded. A shell export, a systemd
  # Environment= line and a heredoc all leave it behind, and `1 ` matched none of the words below,
  # so it fell through to the default -- writing a flag ON left it OFF, and writing one OFF left it
  # ON. No value this accepts contains whitespace, so deleting it cannot hide one.
  case "$(printf '%s' "${1:-}" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    0|false|no|off) return 1 ;;
    *) [ "${2:-1}" = "1" ] ;;
  esac
}


ts_profile_perf_flags() {
  # WAL: binary framing. Records are protobuf unconditionally now, so there is no
  # longer a variable for that half of it.
  export TS_WAL_BINARY_FRAME="${TS_WAL_BINARY_FRAME:-1}"
