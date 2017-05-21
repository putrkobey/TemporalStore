#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Validate repository-level open-source readiness files."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

# THE ROOT COPIES ARE REQUIRED TOO, and that is not tidiness.
#
# This list named only the `docs/` pair, so the gate reported success on a tree with no
# `CONTRIBUTING.md` at the root -- the copy GitHub surfaces to someone arriving at the
# project. It was found by planting that deletion, not by reading the list, which is the
# only way a passing check is ever shown to work.
#
# The two pairs are not the same documents. The root `CONTRIBUTING.md` is 105 lines and
# says pull requests are welcome "from anyone"; `docs/CONTRIBUTING.md` is 67 lines. The
# root `CODE_OF_CONDUCT.md` adopts the Contributor Covenant 2.1 by name and link in 42
# lines; the `docs/` one is 13. So the fuller pair was the unchecked pair, and either copy
# could have been deleted or left to rot behind a green gate.
#
# Which copy should be canonical -- and whether one should become a pointer to the other --
# is a content decision and is NOT made here. Requiring both makes the gate cover what it
# already claimed to cover, and that does not need the question settled.
REQUIRED_FILES = [
    "README.md",
    "LICENSE",
    "NOTICE",
    "CONTRIBUTING.md",
    "CODE_OF_CONDUCT.md",
    "docs/CONTRIBUTING.md",
    "docs/SECURITY.md",
    "docs/CODE_OF_CONDUCT.md",
    ".gitignore",
    "docs/open_source_readiness.md",
]

README_TOKENS = [
    "Apache-2.0",
    "rust-main",
    "no brpc/thrift",
    "Rust-native",
    "production-readiness claims",
]

SECURITY_TOKENS = [
    "Do not open a public issue with exploit details",
    "Do not commit API keys",
    "TS_RAFT_AUTH_TOKEN",
]

CONTRIBUTING_TOKENS = [
    "shared-corpus",
    "rust-internal",
    "Do not add brpc or thrift",
]

IGNORE_TOKENS = [
    "target/",
    ".codex/",
    ".local/",
    "build-ubuntu22/",
    "output/",
    "target-rust-bench/",
    "thirdparty/",
    "benchmark_reports/",
]

# Keep private account markers out of the published rule source itself.
PRIVATE_ACCOUNT_MARKER = bytes.fromhex("64656570726f757465").decode("ascii")

PRIVATE_PATH_TOKENS = [
    "C:\\Users\\",
    "/mnt/c/Users",
    "/mnt/c/Users/",
    "/root/src/",
    "Vincent Jiang",
]

FALLBACK_SCAN_SKIP_DIRS = {
    ".git",
    "target",
    "target-rust-bench",
    "build-ubuntu22",
    "output",
    "benchmark_reports",
    "thirdparty",
    "__pycache__",
}

# Names of products, toolchains and internal systems that do not belong in an
# open-source repository. Nothing else in CI has an opinion about prose, so a
# change can add hundreds of occurrences and still pass every other gate.
#
# `matrixobjectstore` is deliberately NOT here: it is the published name of an
# Apache-2.0 crate this tree depends on, so it appears in a Cargo.toml dependency, in
# the feature that enables it and in the bridge whose name is public API.
# validate_no_enterprise_object_store draws the distinction that matters -- depending on
# that crate is ordinary, containing a copy of it is not -- and a blanket match here
# would fail on all four.
FOREIGN_VOCABULARY: list[tuple[str, str, int]] = [
    (
        "language and toolchain names",
        r"c\+\+|cplusplus|c_plus_plus|(?<![A-Za-z])cxx(?![A-Za-z])"
        r"|(?<![A-Za-z])cpp(?![A-Za-z])|(?<![A-Za-z])gtest(?![A-Za-z])",
        re.IGNORECASE,
    ),
    (
        "third-party product names",
        r"(?<![A-Za-z])(lark|feishu|wukong|bytedance|byteraft|bytestore|mtcache"
        r"|openviking|vikingmem)(?![A-Za-z])|volcano engine",
        re.IGNORECASE,
    ),
    # "abase" is a substring of "database", so it only counts on a token boundary.
    ("internal datastore name", r"(?<![A-Za-z])abase(?![A-Za-z])", re.IGNORECASE),
    # The C-FFI spelling is CIpsFoo, which a token-start-only pattern misses, and
    # this one is case-sensitive on purpose: "ips" alone is ordinary English.
    ("retired data-model name", r"(?<![A-Za-z0-9])(C?Ips[A-Z]|ips_|IPS(?![a-zA-Z]))", 0),
]

# These files carry the patterns above as fixtures or as the check itself, in the
# same way PRIVATE_PATH_FIXTURE_FILES carries the private-path patterns.
FOREIGN_VOCABULARY_FIXTURE_FILES = {
    "tools/validate_open_source_readiness.py",
}

PRIVATE_PATH_FIXTURE_FILES = {
    "tools/validate_open_source_readiness.py",
    "tools/validate_temporalstore_performance_execution_redaction.py",
    "tools/test_temporalstore_performance_execution_redaction.py",
}

def read(relative_path: str) -> str:
    path = ROOT / relative_path
    if not path.exists():
        raise SystemExit(f"missing required file: {relative_path}")
    return path.read_text(encoding="utf-8", errors="replace")


def require_tokens(relative_path: str, tokens: list[str]) -> None:
    text = read(relative_path)
    normalized = " ".join(text.split())
    missing = [token for token in tokens if token not in text and token not in normalized]
    if missing:
        raise SystemExit(f"{relative_path} missing tokens: {', '.join(missing)}")


def validate_cargo_license() -> None:
    cargo = read("Cargo.toml")
    if 'license = "Apache-2.0"' not in cargo:
        raise SystemExit("Cargo.toml must declare workspace Apache-2.0 license")
    for crate in ["crates/temporalstore-rust/Cargo.toml", "crates/temporalstore-snapshot/Cargo.toml"]:
        text = read(crate)
        if "license.workspace = true" not in text:
            raise SystemExit(f"{crate} must inherit workspace license")


def validate_license() -> None:
    license_text = read("LICENSE")
    if "Apache License" not in license_text or "Version 2.0" not in license_text:
        raise SystemExit("LICENSE must contain Apache License 2.0 text")
    notice = read("NOTICE")
    if "MatrixArkAI contributors" not in notice:
        raise SystemExit("NOTICE must include contributor attribution")


def validate_no_tracked_local_codex_config() -> None:
    tracked = repository_files()
    local_codex_files = [path for path in tracked if path == ".codex" or path.startswith(".codex/")]
    if local_codex_files:
        raise SystemExit(
            "tracked local Codex config is not open-source safe: "
            + ", ".join(local_codex_files)
        )


def validate_no_private_paths() -> None:
    tracked = repository_files()
    offenders: list[str] = []
    for relative_path in tracked:
