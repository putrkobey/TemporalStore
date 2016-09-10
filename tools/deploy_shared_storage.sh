#!/usr/bin/env bash
# =============================================================================
# deploy_shared_storage.sh - many nodes over one authoritative shared store.
#
# The only profile with a distance to span. The durable copy does not live on
# the node, so shard data follows shards on rebalance and survives losing a
# datanode outright - and reads that miss memory would otherwise cross a network
# to reach it. That is what the on-disk cache tier is for, and this is the only
# profile that opens one.
#
# The tier is not forced on here. The backend rule turns it on for exactly the
# shared-storage backends, so the storage the node opens and the storage this
# script provisions come from one decision rather than two that can disagree.
#
# Usage (per node), against a shared filesystem root:
