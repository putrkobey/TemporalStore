#!/usr/bin/env bash
# MatrixArk Memory plugin — Claude Code lifecycle hook wrapper.
#
# Routes a plugin hook event to the MatrixArk / TemporalStore engine
# (tools/matrixark_claude_hook.sh in your TemporalStore checkout).
#
# Fail-open by design: a memory layer must never block a turn. If the engine
# is not configured or not present, emit a benign result and exit 0.
