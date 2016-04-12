#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Private account markers must be rejected even in validator fixtures."""
import unittest
from unittest.mock import patch
import validate_open_source_readiness as readiness

class AccountMarkerTest(unittest.TestCase):
    def test_content_including_fixture_files_is_rejected_in_any_case(self):
        marker = bytes.fromhex("64656570726f757465").decode("ascii")
        for name in ["README.md", "tools/validate_open_source_readiness.py"]:
            for text in [marker, marker.upper(), marker.title(), "prefix" + marker + "suffix"]:
                with self.subTest(name=name, text=text):
                    with patch.object(readiness, "repository_files", return_value=[name]), patch.object(readiness.Path, "is_file", return_value=True), patch.object(readiness.Path, "read_text", return_value=text):
