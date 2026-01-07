# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import builtins
import os
from pathlib import Path
from typing import Any, Callable, Generator, Optional
from unittest.mock import mock_open

import pytest


@pytest.fixture  # type: ignore[misc]
def mock_manifest_open() -> Callable[[Optional[str]], Callable[..., Any]]:
    """
    Returns a factory for a side_effect that mocks open() for manifest CSVs
    but delegates to real open() for everything else (like dlt state files).

    Usage:
    def test_foo(mock_manifest_open):
        with patch("builtins.open", side_effect=mock_manifest_open(read_data="...")):
             ...
    """

    def _factory(read_data: Optional[str] = None) -> Callable[..., Any]:
        original_open = builtins.open
        mo = mock_open(read_data=read_data)

        def side_effect(file: Any, *args: Any, **kwargs: Any) -> Any:
            # Target our manifest files. They are usually simple strings ending in .csv
            # or simple paths like "path.csv".
            # dlt state files are usually absolute paths involving .dlt/ or temp dirs.
            if isinstance(file, str) and file.endswith(".csv"):
                return mo(file, *args, **kwargs)
            return original_open(file, *args, **kwargs)

        return side_effect

    return _factory


@pytest.fixture(autouse=True)  # type: ignore[misc]
def clean_dlt_environment(tmp_path: Path) -> Generator[None, None, None]:
    """
    Ensures each test runs with a fresh DLT environment to prevent state leakage.
    Sets the pipeline directory to a temporary path.
    """
    # Save original env
    orig_pipeline_dir = os.environ.get("DLT_PIPELINE_DIR")

    # Set temp pipeline dir
    os.environ["DLT_PIPELINE_DIR"] = str(tmp_path / ".dlt")

    yield

    # Restore
    if orig_pipeline_dir is None:
        del os.environ["DLT_PIPELINE_DIR"]
    else:
        os.environ["DLT_PIPELINE_DIR"] = orig_pipeline_dir
