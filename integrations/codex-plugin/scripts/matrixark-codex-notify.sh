#!/usr/bin/env bash
# MatrixArk memory — Codex `notify` handler.
#
# Codex calls this with a single JSON argument describing the completed turn:
#   { "type": "agent-turn-complete", "thread-id": ..., "turn-id": ..., "cwd": ...,
#     "input-messages": [...], "last-assistant-message": "..." }
#
