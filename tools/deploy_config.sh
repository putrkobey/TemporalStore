#!/usr/bin/env bash
# =============================================================================
# deploy_config.sh - install the centralized config file + launcher on a node
# and wire the running systemd units to read it ON START. Idempotent.
#
# Run this ONCE per node on the NEXT cluster start (the instances persist
# /opt/temporalstore on EBS across stop/start, so this only needs re-running
# when the config file or units change). It:
#
#   1. copies config/temporalstore.toml, scripts/with_config.sh and
#      tools/matrixark_load_config.py from this repo checkout into the canonical
#      /opt/temporalstore tree (never overwrites an existing edited config
#      unless --force-config is given);
#   2. for each installed matrixark-* unit, writes an idempotent drop-in that
#      re-points ExecStart THROUGH the launcher, preserving the unit's current
#      command + args (read live from systemd) so nothing else changes;
#   3. daemon-reload (and --restart to restart the wired units now).
#
# Precedence is preserved: systemd applies a unit's Environment= /
# EnvironmentFile= before ExecStart, so with_config.sh sees those as already-set
# and does not override them (explicit env wins over the config file).
#
# Usage (on the node, as root):
#   sudo /opt/temporalstore/tools/deploy_config.sh [--src <repo>] [--force-config] [--restart]
#   sudo tools/deploy_config.sh                 # from a repo checkout
# =============================================================================
set -euo pipefail

PREFIX="/opt/temporalstore"
CONF_DST="${PREFIX}/config/temporalstore.toml"
DROPIN_NAME="10-with-config.conf"
UNITS=(matrixark-metaserver matrixark-proxy matrixark-datanode matrixark-gateway)

src=""
force_config=0
do_restart=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --src) src="${2:?}"; shift 2 ;;
    --force-config) force_config=1; shift ;;
    --restart) do_restart=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# Resolve the repo checkout this script ships in (…/tools/deploy_config.sh).
if [[ -z "$src" ]]; then
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  src="$(cd "${script_dir}/.." && pwd)"
fi

echo "[deploy-config] source repo: ${src}"
for f in config/temporalstore.toml scripts/with_config.sh tools/matrixark_load_config.py; do
  [[ -f "${src}/${f}" ]] || { echo "[deploy-config] MISSING ${src}/${f}" >&2; exit 1; }
