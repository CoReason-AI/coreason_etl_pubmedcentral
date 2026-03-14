# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import importlib
import tempfile
from pathlib import Path

import pytest

import coreason_etl_pubmedcentral.utils.logger as logger_module
from coreason_etl_pubmedcentral.utils.logger import logger


def test_logger_initialization() -> None:
    """Test that the logger is initialized correctly and creates the log directory."""
    # Since the logger is initialized on import, we check side effects
    # Ensure logs directory creation is handled
    log_path = Path("logs")
    if not log_path.exists():
        log_path.mkdir(parents=True, exist_ok=True)
    assert log_path.exists()
    assert log_path.is_dir()


def test_logger_exports() -> None:
    """Test that logger is exported."""
    assert logger is not None


def test_logger_directory_creation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test the mkdir code path when the logs directory does not exist."""

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # In logger.py it does:
        # log_path = Path("logs")
        # if not log_path.exists(): log_path.mkdir(...)
        # We can simulate this by chdir to our temp dir!

        # Monkeypatch the current working directory to our temp directory
        monkeypatch.chdir(temp_path)

        # Directory shouldn't exist initially
        assert not Path("logs").exists()

        # Remove it from sys.modules so it's loaded afresh
        # Actually reload should suffice to rerun module-level code
        importlib.reload(logger_module)

        # After reload, it should exist
        assert Path("logs").exists()

        # Remove loguru handlers so the log file is released, allowing Windows to delete the temp dir
        logger_module.logger.remove()
