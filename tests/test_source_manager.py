# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from tenacity import stop_after_attempt

from coreason_etl_pubmedcentral.source_manager import (
    FTPSource,
    S3Source,
    SourceConnectionError,
    SourceFileNotFoundError,
    SourceManager,
    SourceZeroByteError,
)


class TestSourceManager(unittest.TestCase):
    def setUp(self) -> None:
        self.patcher_s3 = patch("coreason_etl_pubmedcentral.source_manager.S3Source")
        self.patcher_ftp = patch("coreason_etl_pubmedcentral.source_manager.FTPSource")
        self.mock_s3_class = self.patcher_s3.start()
        self.mock_ftp_class = self.patcher_ftp.start()

        self.mock_s3 = MagicMock()
        self.mock_ftp = MagicMock()

        self.mock_s3_class.return_value = self.mock_s3
        self.mock_ftp_class.return_value = self.mock_ftp

        self.manager = SourceManager()

    def tearDown(self) -> None:
        self.patcher_s3.stop()
        self.patcher_ftp.stop()

    def test_get_file_s3_success(self) -> None:
        """Test successful S3 download resets error count and returns path."""
        self.mock_s3.get_file.return_value = "fake_s3.tar.gz"
        self.manager.s3_error_count = 2  # Should be reset to 0

        result = self.manager.get_file("test/file.xml")

        assert result == "fake_s3.tar.gz"
        self.mock_s3.get_file.assert_called_once_with("test/file.xml")
        self.mock_ftp.get_file.assert_not_called()
        assert self.manager.s3_error_count == 0
        assert not self.manager.is_failover_active

    def test_get_file_s3_connection_error_fallback(self) -> None:
        """Test single-file fallback to FTP on S3 connection error."""
        self.mock_s3.get_file.side_effect = SourceConnectionError("Conn error")
        self.mock_ftp.get_file.return_value = "fake_ftp.tar.gz"

        result = self.manager.get_file("test/file.xml")

        assert result == "fake_ftp.tar.gz"
        self.mock_s3.get_file.assert_called_once_with("test/file.xml")
        self.mock_ftp.get_file.assert_called_once_with("test/file.xml")
        assert self.manager.s3_error_count == 1
        assert not self.manager.is_failover_active

    def test_get_file_s3_persistent_failover(self) -> None:
        """Test persistent failover to FTP after 3 S3 connection errors."""
        self.mock_s3.get_file.side_effect = SourceConnectionError("Conn error")
        self.mock_ftp.get_file.return_value = "fake_ftp.tar.gz"

        for i in range(3):
            result = self.manager.get_file(f"test/file_{i}.xml")
            assert result == "fake_ftp.tar.gz"
            assert self.manager.s3_error_count == i + 1

        assert self.manager.is_failover_active

        # 4th call should go straight to FTP
        self.mock_s3.get_file.reset_mock()
        self.mock_ftp.get_file.reset_mock()

        result = self.manager.get_file("test/file_3.xml")
        assert result == "fake_ftp.tar.gz"
        self.mock_s3.get_file.assert_not_called()
        self.mock_ftp.get_file.assert_called_once_with("test/file_3.xml")

    def test_get_file_s3_file_not_found_fallback(self) -> None:
        """Test single-file fallback to FTP on S3 file not found, without incrementing error count."""
        self.mock_s3.get_file.side_effect = SourceFileNotFoundError("Not found")
        self.mock_ftp.get_file.return_value = "fake_ftp.tar.gz"

        result = self.manager.get_file("test/file.xml")

        assert result == "fake_ftp.tar.gz"
        assert self.manager.s3_error_count == 0
        assert not self.manager.is_failover_active

    def test_get_file_s3_zero_byte_fallback(self) -> None:
        """Test single-file fallback to FTP on S3 zero byte error, without incrementing error count."""
        self.mock_s3.get_file.side_effect = SourceZeroByteError("Zero byte")
        self.mock_ftp.get_file.return_value = "fake_ftp.tar.gz"

        result = self.manager.get_file("test/file.xml")

        assert result == "fake_ftp.tar.gz"
        assert self.manager.s3_error_count == 0
        assert not self.manager.is_failover_active


class TestS3Source(unittest.TestCase):
    def setUp(self) -> None:
        self.patcher_fs = patch("coreason_etl_pubmedcentral.source_manager.fsspec.filesystem")
        self.mock_fsspec = self.patcher_fs.start()
        self.mock_fs = MagicMock()
        self.mock_fsspec.return_value = self.mock_fs
        self.source = S3Source()

    def tearDown(self) -> None:
        self.patcher_fs.stop()

    def test_download_success(self) -> None:
        """Test successful download."""

        def mock_get(_s3_path: str, local_path: str) -> None:
            with open(local_path, "wb") as f:
                f.write(b"data")

        self.mock_fs.get.side_effect = mock_get

        # Override retry logic for tests to run faster
        self.source._download.__func__.retry.stop = stop_after_attempt(1)  # type: ignore[attr-defined]

        local_path = self.source.get_file("test.tar.gz")
        assert Path(local_path).exists()
        assert Path(local_path).stat().st_size > 0
        os.remove(local_path)

    def test_download_file_not_found(self) -> None:
        """Test FileNotFoundError is wrapped."""
        self.mock_fs.get.side_effect = FileNotFoundError()
        self.source._download.__func__.retry.stop = stop_after_attempt(1)  # type: ignore[attr-defined]

        with pytest.raises(SourceFileNotFoundError):
            self.source.get_file("test.tar.gz")

    def test_download_zero_byte(self) -> None:
        """Test SourceZeroByteError is raised on 0 byte files."""

        def mock_get(_s3_path: str, local_path: str) -> None:
            with open(local_path, "wb"):
                pass  # Empty file

        self.mock_fs.get.side_effect = mock_get
        self.source._download.__func__.retry.stop = stop_after_attempt(1)  # type: ignore[attr-defined]

        with pytest.raises(SourceZeroByteError):
            self.source.get_file("test.tar.gz")

    def test_download_connection_error(self) -> None:
        """Test generic Exception is wrapped in SourceConnectionError."""
        self.mock_fs.get.side_effect = Exception("Generic error")
        self.source._download.__func__.retry.stop = stop_after_attempt(1)  # type: ignore[attr-defined]

        with pytest.raises(SourceConnectionError):
            self.source.get_file("test.tar.gz")


class TestFTPSource(unittest.TestCase):
    def setUp(self) -> None:
        self.patcher_fs = patch("coreason_etl_pubmedcentral.source_manager.fsspec.filesystem")
        self.mock_fsspec = self.patcher_fs.start()
        self.mock_fs = MagicMock()
        self.mock_fsspec.return_value = self.mock_fs
        self.source = FTPSource()

    def tearDown(self) -> None:
        self.patcher_fs.stop()

    def test_download_success(self) -> None:
        """Test successful download."""

        def mock_get(_ftp_path: str, local_path: str) -> None:
            with open(local_path, "wb") as f:
                f.write(b"data")

        self.mock_fs.get.side_effect = mock_get
        self.source._download.__func__.retry.stop = stop_after_attempt(1)  # type: ignore[attr-defined]

        local_path = self.source.get_file("test.tar.gz")
        assert Path(local_path).exists()
        assert Path(local_path).stat().st_size > 0
        os.remove(local_path)

    def test_download_file_not_found(self) -> None:
        """Test FileNotFoundError is wrapped."""
        self.mock_fs.get.side_effect = FileNotFoundError()
        self.source._download.__func__.retry.stop = stop_after_attempt(1)  # type: ignore[attr-defined]

        with pytest.raises(SourceFileNotFoundError):
            self.source.get_file("test.tar.gz")

    def test_download_zero_byte(self) -> None:
        """Test SourceZeroByteError is raised on 0 byte files."""

        def mock_get(_ftp_path: str, local_path: str) -> None:
            with open(local_path, "wb"):
                pass  # Empty file

        self.mock_fs.get.side_effect = mock_get
        self.source._download.__func__.retry.stop = stop_after_attempt(1)  # type: ignore[attr-defined]

        with pytest.raises(SourceZeroByteError):
            self.source.get_file("test.tar.gz")

    def test_download_connection_error(self) -> None:
        """Test generic Exception is wrapped in SourceConnectionError."""
        self.mock_fs.get.side_effect = Exception("Generic error")
        self.source._download.__func__.retry.stop = stop_after_attempt(1)  # type: ignore[attr-defined]

        with pytest.raises(SourceConnectionError):
            self.source.get_file("test.tar.gz")
