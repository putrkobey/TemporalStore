#!/usr/bin/env bash
# =============================================================================
# deploy_raft.sh - many nodes, each on memory + its own local disk (EBS).
#
# This is what scaling out means by default. Every node keeps a full durable
# copy on its own volume and the raft log keeps those copies in step, so the
# storage shape per node is identical to one-box - memory plus local disk, no
# on-disk cache tier, because a node's durable copy is already under it.
#
# What changes against one-box is node count, not storage: a metaserver to
# register with, a node id that must be unique, and peers to replicate to.
# Shared storage is the opt-in alternative (see deploy_shared_storage.sh); a
# node told to be distributed and nothing else lands here.
#
# Usage (per node):
#   TS_META_ADDR=10.0.0.10:17001 TS_SERVER_NODE_ID=2 TS_SHARD_ID=1 \
#   TS_SERVER_ADDR=10.0.0.11:17002 sudo -E tools/deploy_raft.sh
