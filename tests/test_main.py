# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import sys
from unittest.mock import patch

import pytest

from coreason_etl_pubmedcentral.main import cli, run_pipeline


def test_run_pipeline() -> None:
    """Test the run_pipeline stub doesn't crash."""
    run_pipeline("dummy_path.csv")


def test_cli_with_argument() -> None:
    """Test the CLI with a valid positional argument."""
    with (
        patch.object(sys, "argv", ["pmc-etl", "my_manifest.csv"]),
        patch("coreason_etl_pubmedcentral.main.run_pipeline") as mock_run,
    ):
        cli()
        mock_run.assert_called_once_with("my_manifest.csv")


def test_cli_without_argument() -> None:
    """Test the CLI without any arguments raises SystemExit."""
    with patch.object(sys, "argv", ["pmc-etl"]), pytest.raises(SystemExit):
        cli()
