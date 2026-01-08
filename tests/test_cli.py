# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import sys
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest

from coreason_etl_pubmedcentral.main import cli


@pytest.fixture  # type: ignore[misc]
def mock_run_pipeline() -> Generator[MagicMock, None, None]:
    with patch("coreason_etl_pubmedcentral.main.run_pipeline") as mock:
        yield mock


def test_cli_success(mock_run_pipeline: MagicMock) -> None:
    """Verify CLI parses arguments and calls run_pipeline correctly."""
    test_args = [
        "prog",
        "manifest.csv",
        "--destination",
        "postgres",
        "--dataset-name",
        "test_ds",
        "--remote-manifest-path",
        "s3://bucket/man.csv",
    ]
    with patch.object(sys, "argv", test_args):
        cli()

    mock_run_pipeline.assert_called_once_with(
        manifest_path="manifest.csv",
        destination="postgres",
        dataset_name="test_ds",
        remote_manifest_path="s3://bucket/man.csv",
    )


def test_cli_defaults(mock_run_pipeline: MagicMock) -> None:
    """Verify CLI uses correct defaults."""
    test_args = ["prog", "manifest.csv"]
    with patch.object(sys, "argv", test_args):
        cli()

    mock_run_pipeline.assert_called_once_with(
        manifest_path="manifest.csv",
        destination="duckdb",
        dataset_name="pmc_data",
        remote_manifest_path=None,
    )


def test_cli_execution_failure(mock_run_pipeline: MagicMock) -> None:
    """
    Verify CLI handles exceptions and exits with 1.
    Ensures robust error handling for user-facing commands.
    """
    mock_run_pipeline.side_effect = RuntimeError("Crash")
    test_args = ["prog", "manifest.csv"]

    with patch.object(sys, "argv", test_args):
        with pytest.raises(SystemExit) as excinfo:
            cli()
        assert excinfo.value.code == 1
