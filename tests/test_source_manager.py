# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import pytest
from unittest.mock import MagicMock, patch
from coreason_etl_pubmedcentral.source_manager import SourceManager, SourceType

@pytest.fixture
def mock_fsspec_filesystem():
    with patch("fsspec.filesystem") as mock_fs_factory:
        mock_s3 = MagicMock()
        mock_ftp = MagicMock()

        def side_effect(protocol, **kwargs):
            if protocol == "s3":
                return mock_s3
            if protocol == "ftp":
                return mock_ftp
            return MagicMock()

        mock_fs_factory.side_effect = side_effect
        yield mock_fs_factory, mock_s3, mock_ftp

@pytest.fixture
def source_manager(mock_fsspec_filesystem):
    return SourceManager()

def test_s3_success(source_manager, mock_fsspec_filesystem):
    _, mock_s3, _ = mock_fsspec_filesystem
    mock_s3.cat.return_value = b"content"

    data = source_manager.get_file("file.xml")

    assert data == b"content"
    assert source_manager._current_source == SourceType.S3
    mock_s3.cat.assert_called_with(f"s3://{SourceManager.S3_BUCKET}/file.xml")

def test_s3_client_error_no_failover(source_manager, mock_fsspec_filesystem):
    """Verify 404 (FileNotFoundError) does not trigger failover."""
    _, mock_s3, mock_ftp = mock_fsspec_filesystem
    mock_s3.cat.side_effect = FileNotFoundError("404 Not Found")

    with pytest.raises(FileNotFoundError):
        source_manager.get_file("missing.xml")

    assert source_manager._current_source == SourceType.S3
    assert source_manager._s3_consecutive_errors == 0
    mock_ftp.cat.assert_not_called()

def test_s3_connection_error_failover(source_manager, mock_fsspec_filesystem):
    """Verify failover after threshold is reached."""
    _, mock_s3, mock_ftp = mock_fsspec_filesystem
    # Simulate connection errors
    mock_s3.cat.side_effect = Exception("Connection Reset")
    mock_ftp.cat.return_value = b"ftp_content"

    # 1st Failure
    with pytest.raises(Exception):
        source_manager.get_file("file1.xml")
    assert source_manager._s3_consecutive_errors == 1

    # 2nd Failure
    with pytest.raises(Exception):
        source_manager.get_file("file2.xml")
    assert source_manager._s3_consecutive_errors == 2

    # 3rd Failure -> Trigger Failover and fetch from FTP immediately?
    # The implementation says: "Fallthrough to FTP immediately for this request" if threshold reached
    data = source_manager.get_file("file3.xml")

    assert data == b"ftp_content"
    assert source_manager._current_source == SourceType.FTP
    mock_ftp.cat.assert_called_with(f"/pub/pmc/file3.xml")

def test_s3_intermittent_failure_resets_counter(source_manager, mock_fsspec_filesystem):
    """Verify success resets the error counter."""
    _, mock_s3, _ = mock_fsspec_filesystem
    mock_s3.cat.side_effect = [Exception("Error"), b"success"]

    with pytest.raises(Exception):
        source_manager.get_file("file1.xml")
    assert source_manager._s3_consecutive_errors == 1

    source_manager.get_file("file2.xml")
    assert source_manager._s3_consecutive_errors == 0

def test_s3_client_error_resets_counter(source_manager, mock_fsspec_filesystem):
    """Verify 404 (ClientError) resets the connection error counter."""
    _, mock_s3, _ = mock_fsspec_filesystem
    mock_s3.cat.side_effect = [Exception("ConnError"), FileNotFoundError("404")]

    with pytest.raises(Exception):
        source_manager.get_file("file1.xml")
    assert source_manager._s3_consecutive_errors == 1

    with pytest.raises(FileNotFoundError):
        source_manager.get_file("file2.xml")
    assert source_manager._s3_consecutive_errors == 0

def test_ftp_usage_after_failover(source_manager, mock_fsspec_filesystem):
    """Verify subsequent requests use FTP after failover."""
    _, _, mock_ftp = mock_fsspec_filesystem
    source_manager._current_source = SourceType.FTP
    mock_ftp.cat.return_value = b"ftp_content"

    data = source_manager.get_file("file.xml")
    assert data == b"ftp_content"
    assert "s3" not in str(mock_ftp.cat.call_args)

def test_close(source_manager, mock_fsspec_filesystem):
    _, mock_s3, mock_ftp = mock_fsspec_filesystem
    source_manager.close()
    mock_s3.clear_instance_cache.assert_called_once()
    mock_ftp.clear_instance_cache.assert_called_once()
