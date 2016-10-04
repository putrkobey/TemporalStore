#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Fail if retired legacy object-store naming leaks into first-party files."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LEGACY_TOKEN = "byte" + "store"
# The two-word spelling has to END there. Without the boundary, `byte\s+store` also matches
# ordinary English -- "cost per byte STORED" in an occupancy comment tripped this guard and is
# what kept it red, and therefore unwired. `stores?` still catches the plural.
PATTERN = re.compile(LEGACY_TOKEN + r"|byte\s+stores?(?![A-Za-z])", re.IGNORECASE)
# The one file allowed to spell the retired name, and why.
#
# `validate_open_source_readiness.py` is the guard whose job is to ENUMERATE forbidden vendor
# names; its regex necessarily contains every one of them. Skipping it is not a hole being hidden:
# a scanner that refuses the dictionary cannot have a dictionary. The entry is narrow -- one named
# file, not a directory or a pattern -- so anything else acquiring the token still fails.
#
# There is a test below asserting this file still contains the token, so the skip cannot quietly
# become dead weight after someone rewrites that guard.
SKIP_FILES = {
    "tools/validate_open_source_readiness.py",
}

SKIP_DIRS = {
    ".git",
    "build-ubuntu22",
    "output-ubuntu22",
    "target",
    ".local",
