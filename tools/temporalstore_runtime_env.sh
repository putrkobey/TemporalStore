#!/usr/bin/env bash
# Shared runtime environment for the launchers that run the Rust engine from a checkout.
#
# `tools/run_rust_unified_tests.sh` and `tools/run_temporalstore_rust_ubuntu22.sh` both source this
# file and call `temporalstore_export_sdk_loader_path`. It was not in the repository, so both
# scripts died on their own line 5 -- `set -euo pipefail` turns a missing `source` into an
# immediate exit, before either script does anything, and the message names a path rather than a
# cause.
#
# The body is the loader-path block that `tools/matrixark_codex_rust_hook.sh` already runs inline,
# lifted into the function those two scripts expect rather than invented: the SDK builds a cdylib,
