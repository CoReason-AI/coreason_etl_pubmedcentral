# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import abc
import os
import tempfile
from pathlib import Path

import fsspec
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from coreason_etl_pubmedcentral.utils.logger import logger


class SourceFetchError(Exception):
    """Base exception for source fetching errors."""


class SourceFileNotFoundError(SourceFetchError):
    """Exception raised when a file is not found on the source."""


class SourceConnectionError(SourceFetchError):
    """Exception raised when there is a connection error to the source."""


class SourceZeroByteError(SourceFetchError):
    """Exception raised when a zero-byte file is downloaded."""


class BaseSource(abc.ABC):
    """Abstract base class for all file sources."""

    @abc.abstractmethod
    def get_file(self, file_path: str) -> str:
        """
        Download a file from the source to a temporary local file.

        Args:
            file_path: The path of the file to download on the source.

        Returns:
            str: The local file path where the file was downloaded.

        Raises:
            SourceFetchError: If the download fails.
        """
        # pragma: no cover


class S3Source(BaseSource):
    """Source implementation for AWS S3 (pmc-oa-opendata)."""

    def __init__(self, bucket: str = "pmc-oa-opendata"):
        self.bucket = bucket
        # Use anonymous access to public bucket
        self.fs = fsspec.filesystem("s3", anon=True)

    @retry(  # type: ignore[misc]
        retry=retry_if_exception_type((SourceConnectionError, TimeoutError, ConnectionRefusedError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _download(self, s3_path: str, local_path: str) -> None:
        try:
            self.fs.get(s3_path, local_path)
            # Verify file size
            if Path(local_path).stat().st_size == 0:
                raise SourceZeroByteError(f"Downloaded file {s3_path} is empty (0 bytes)")
        except FileNotFoundError as e:
            raise SourceFileNotFoundError(f"File not found: {s3_path}") from e
        except SourceZeroByteError:
            raise
        except Exception as e:
            # Wrap other exceptions as connection errors to trigger tenacity retries
            raise SourceConnectionError(f"Connection error downloading {s3_path}: {e}") from e

    def get_file(self, file_path: str) -> str:
        """Download file from S3."""
        s3_path = f"s3://{self.bucket}/{file_path}"
        # Create temp file and let it persist
        fd, local_path = tempfile.mkstemp(suffix=".tar.gz")
        os.close(fd)

        logger.info(f"Downloading from S3: {s3_path}")
        self._download(s3_path, local_path)
        logger.info(f"Successfully downloaded to {local_path}")
        return local_path


class FTPSource(BaseSource):
    """Source implementation for NCBI FTP server."""

    def __init__(self, host: str = "ftp.ncbi.nlm.nih.gov", base_path: str = "/pub/pmc/"):
        self.host = host
        self.base_path = base_path.rstrip("/")
        self.fs = fsspec.filesystem("ftp", host=self.host)

    @retry(  # type: ignore[misc]
        retry=retry_if_exception_type((SourceConnectionError, TimeoutError, ConnectionRefusedError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _download(self, ftp_path: str, local_path: str) -> None:
        try:
            # We must use explicit path for fsspec FTP
            self.fs.get(ftp_path, local_path)
            # Verify file size
            if Path(local_path).stat().st_size == 0:
                raise SourceZeroByteError(f"Downloaded file {ftp_path} is empty (0 bytes)")
        except FileNotFoundError as e:
            raise SourceFileNotFoundError(f"File not found: {ftp_path}") from e
        except SourceZeroByteError:
            raise
        except Exception as e:
            raise SourceConnectionError(f"Connection error downloading {ftp_path}: {e}") from e

    def get_file(self, file_path: str) -> str:
        """Download file from FTP."""
        # Ensure path is relative to the base_path
        file_path = file_path.lstrip("/")
        ftp_path = f"{self.base_path}/{file_path}"

        # Create temp file and let it persist
        fd, local_path = tempfile.mkstemp(suffix=".tar.gz")
        os.close(fd)

        logger.info(f"Downloading from FTP: {ftp_path}")
        self._download(ftp_path, local_path)
        logger.info(f"Successfully downloaded to {local_path}")
        return local_path


class SourceManager:
    """
    Manages dual-source downloading with failover and fallback.

    Implements:
    - S3 Primary (Anonymous/Public)
    - FTP Secondary (Failover)
    - Persistent Failover: 3 consecutive S3 connection errors switches strictly to FTP
    - Single-File Fallback: Single file fetch failure falls back to FTP for that file.
    - Zero-byte graceful handling
    """

    def __init__(
        self, s3_bucket: str = "pmc-oa-opendata", ftp_host: str = "ftp.ncbi.nlm.nih.gov", ftp_path: str = "/pub/pmc/"
    ):
        self.s3_source = S3Source(bucket=s3_bucket)
        self.ftp_source = FTPSource(host=ftp_host, base_path=ftp_path)

        self.s3_error_count = 0
        self.failover_threshold = 3
        self.is_failover_active = False

    def get_file(self, file_path: str) -> str:
        """
        Download a file, adhering to the dual-source architecture rules.
        """
        if self.is_failover_active:
            logger.info("Persistent failover active. Using FTP source directly.")
            return self.ftp_source.get_file(file_path)

        try:
            local_path = self.s3_source.get_file(file_path)
            # Reset error count on success
            self.s3_error_count = 0
            return local_path

        except SourceConnectionError as e:
            self.s3_error_count += 1
            logger.warning(f"S3 Connection Error ({self.s3_error_count}/{self.failover_threshold}): {e}")

            if self.s3_error_count >= self.failover_threshold:
                logger.info("FailoverEvent - S3 unreachable. Switched to FTP persistently.")
                self.is_failover_active = True

            logger.info(f"Falling back to FTP for file {file_path}")
            # Single file fallback triggered either by connection error below threshold
            # or by reaching the threshold just now.
            return self.ftp_source.get_file(file_path)

        except (SourceFileNotFoundError, SourceZeroByteError) as e:
            logger.warning(f"S3 Retrieval Error: {e}. Falling back to FTP for {file_path}")
            # Single file fallback without incrementing the global failover error count
            return self.ftp_source.get_file(file_path)
