# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import io
from unittest.mock import MagicMock, patch

import pytest

from coreason_etl_pubmedcentral.config import PubMedCentralConfig
from coreason_etl_pubmedcentral.source_manager import EpistemicSourceManagerPolicy


@pytest.fixture
def mock_config() -> PubMedCentralConfig:
    config = PubMedCentralConfig()
    config.s3_max_retry_attempts = 3
    config.s3_bucket = "pmc-oa-opendata"
    config.ftp_host = "ftp.ncbi.nlm.nih.gov"
    config.ftp_path = "/pub/pmc/"
    return config


def test_source_manager_s3_success(mock_config: PubMedCentralConfig) -> None:
    """Test successful S3 fetch."""
    manager = EpistemicSourceManagerPolicy(mock_config)

    with patch.object(manager.s3_source.fs, "open") as mock_open:
        mock_file = MagicMock()
        mock_file.__enter__.return_value.read.return_value = b"success data"
        mock_open.return_value = mock_file

        data, source = manager.get_file("test.tar.gz")

        assert data.read() == b"success data"
        assert source == "S3"
        assert manager.consecutive_s3_failures == 0
        assert not manager.circuit_breaker_tripped


def test_source_manager_s3_failure_falls_back_to_ftp(mock_config: PubMedCentralConfig) -> None:
    """Test that a single S3 failure instantly falls back to FTP for the specific file."""
    manager = EpistemicSourceManagerPolicy(mock_config)

    # We must patch the underlying implementation of the retry to avoid tenacity infinite loops in tests
    with (
        patch.object(manager.s3_source, "_fetch_file_impl", side_effect=OSError("S3 failed")),
        patch.object(manager.ftp_source.fs, "open") as mock_open_ftp,
    ):
        mock_ftp_file = MagicMock()
        mock_ftp_file.__enter__.return_value.read.return_value = b"ftp data"
        mock_open_ftp.return_value = mock_ftp_file

        # We also need to patch the tenacity decorator to fail fast for tests
        manager.s3_source.fetch_file.retry.stop = lambda _retry_state: True  # type: ignore[attr-defined]

        data, source = manager.get_file("test.tar.gz")

        assert data.read() == b"ftp data"
        assert source == "FTP"
        assert manager.consecutive_s3_failures == 1
        assert not manager.circuit_breaker_tripped


def test_source_manager_circuit_breaker_tripped(mock_config: PubMedCentralConfig) -> None:
    """Test that 3 consecutive S3 failures trip the circuit breaker persistently."""
    manager = EpistemicSourceManagerPolicy(mock_config)

    with (
        patch.object(manager.s3_source, "_fetch_file_impl", side_effect=OSError("S3 failed")),
        patch.object(manager.ftp_source, "fetch_file", return_value=(io.BytesIO(b"ftp data"), "FTP")),
    ):
        manager.s3_source.fetch_file.retry.stop = lambda _retry_state: True  # type: ignore[attr-defined]

        # 1st failure
        manager.get_file("test1.tar.gz")
        assert manager.consecutive_s3_failures == 1
        assert not manager.circuit_breaker_tripped

        # 2nd failure
        manager.get_file("test2.tar.gz")
        assert manager.consecutive_s3_failures == 2
        assert not manager.circuit_breaker_tripped

        # 3rd failure (trips breaker)
        manager.get_file("test3.tar.gz")
        assert manager.consecutive_s3_failures == 3
        assert manager.circuit_breaker_tripped

        # 4th request skips S3 entirely
        data, source = manager.get_file("test4.tar.gz")
        assert data.read() == b"ftp data"
        assert source == "FTP"
        # Since it bypassed S3, the consecutive failures counter shouldn't change
        assert manager.consecutive_s3_failures == 3


def test_source_manager_s3_success_resets_counter(mock_config: PubMedCentralConfig) -> None:
    """Test that an intermittent S3 success resets the error counter to zero."""
    manager = EpistemicSourceManagerPolicy(mock_config)

    # Force 1 failure
    with (
        patch.object(manager.s3_source, "_fetch_file_impl", side_effect=OSError("S3 failed")),
        patch.object(manager.ftp_source, "fetch_file", return_value=(io.BytesIO(b"ftp data"), "FTP")),
    ):
        manager.s3_source.fetch_file.retry.stop = lambda _retry_state: True  # type: ignore[attr-defined]
        manager.get_file("test1.tar.gz")
        assert manager.consecutive_s3_failures == 1

    # Then success
    with patch.object(manager.s3_source, "fetch_file", return_value=(io.BytesIO(b"s3 data"), "S3")):
        data, source = manager.get_file("test2.tar.gz")
        assert data.read() == b"s3 data"
        assert source == "S3"
        # Counter must reset to 0
        assert manager.consecutive_s3_failures == 0
        assert not manager.circuit_breaker_tripped


def test_ftp_source_error_handling(mock_config: PubMedCentralConfig) -> None:
    """Test FTP Source error handling."""
    manager = EpistemicSourceManagerPolicy(mock_config)

    with (
        patch.object(manager.ftp_source.fs, "open", side_effect=Exception("FTP Read Error")),
        pytest.raises(OSError, match="FTP Read Error"),
    ):
        manager.ftp_source.fetch_file("test.tar.gz")


def test_s3_source_not_implemented(mock_config: PubMedCentralConfig) -> None:
    """Test S3 Source fetch_file override is covered."""
    from coreason_etl_pubmedcentral.source_manager import S3Source

    s3 = S3Source(mock_config.s3_bucket, mock_config.s3_max_retry_attempts)

    # Actually call the bound method directly on the class to avoid tenacity
    with pytest.raises(NotImplementedError, match="This method is overridden in __init__"):
        S3Source.fetch_file(s3, "test.tar.gz")


def test_source_manager_zero_byte_files(mock_config: PubMedCentralConfig) -> None:
    """Test zero-byte file handling on both S3 and FTP."""
    manager = EpistemicSourceManagerPolicy(mock_config)

    with (
        patch.object(manager.s3_source.fs, "open") as mock_open_s3,
        patch.object(manager.ftp_source.fs, "open") as mock_open_ftp,
    ):
        mock_file = MagicMock()
        mock_file.__enter__.return_value.read.return_value = b""  # Zero-byte payload
        mock_open_s3.return_value = mock_file
        mock_open_ftp.return_value = mock_file

        # S3 Source directly
        manager.s3_source.fetch_file.retry.stop = lambda _retry_state: True  # type: ignore[attr-defined]

        # When tenacity is set to fail fast, it raises RetryError unless reraise is true,
        # however we set reraise=True in the tenacity retry decorator. So it should raise OSError directly.
        with pytest.raises(OSError, match="Zero-byte file"):
            manager.s3_source.fetch_file("test.tar.gz")

        # FTP Source directly
        with pytest.raises(OSError, match="Zero-byte file"):
            manager.ftp_source.fetch_file("test.tar.gz")
