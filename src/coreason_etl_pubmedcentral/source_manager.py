# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import io
from enum import Enum, auto
from typing import Any, Optional

import fsspec
from fsspec.implementations.ftp import FTPFileSystem
from s3fs import S3FileSystem

from coreason_etl_pubmedcentral.utils.logger import logger


class SourceType(Enum):
    S3 = auto()
    FTP = auto()


class SourceManager:
    """
    Manages fetching files from S3 with a failover to FTP using fsspec.
    Maintains persistent connections where applicable via fsspec caching.
    """

    S3_BUCKET = "pmc-oa-opendata"
    FTP_HOST = "ftp.ncbi.nlm.nih.gov"
    FTP_BASE_PATH = "/pub/pmc/"
    FAILOVER_THRESHOLD = 3

    def __init__(self) -> None:
        self._current_source = SourceType.S3
        self._s3_consecutive_errors = 0

        # Initialize Filesystems
        # S3: Anonymous access
        self._fs_s3: S3FileSystem = fsspec.filesystem("s3", anon=True)

        # FTP: We initialize it, but connection might happen on first use
        # fsspec's FTPFileSystem handles connection pooling/keepalive internally to some extent
        self._fs_ftp: FTPFileSystem = fsspec.filesystem("ftp", host=self.FTP_HOST)

    def get_file(self, file_path: str) -> bytes:
        """
        Fetches the content of a file given its relative path.
        Example file_path: "oa_comm/xml/PMC12345.xml"
        """
        if self._current_source == SourceType.S3:
            try:
                content = self._fetch_s3(file_path)
                # Success: reset error counter
                self._s3_consecutive_errors = 0
                return content
            except Exception as e:
                # Analyze exception to decide on failover
                # fsspec S3 errors: usually FileNotFoundError for 404, or others for connection.
                # We need to distinguish 404 (ClientError) from connection issues.
                # s3fs usually wraps botocore exceptions or raises standard OSErrors.

                is_connection_error = False
                is_not_found = isinstance(e, FileNotFoundError)

                # Check for underlying botocore connection errors if wrapped
                # s3fs doesn't always wrap cleanly, so we might check message or type name
                # But typically, if it's not FileNotFoundError and not PermissionError, it's likely connection/transient.
                if not is_not_found:
                    is_connection_error = True

                if is_connection_error:
                    self._s3_consecutive_errors += 1
                    logger.warning(
                        f"S3 Error ({self._s3_consecutive_errors}/{self.FAILOVER_THRESHOLD}) "
                        f"fetching {file_path}: {e}"
                    )

                    if self._s3_consecutive_errors >= self.FAILOVER_THRESHOLD:
                        logger.info(
                            f"FailoverEvent — S3 unreachable. Switched to FTP for batch/subsequent requests. "
                            f"Triggered by failure on {file_path}"
                        )
                        self._current_source = SourceType.FTP
                        # Fallthrough to FTP immediately
                    else:
                        # Re-raise so dlt knows this file failed
                        raise e
                else:
                    # ClientError (404/403) -> FileNotFoundError / PermissionError
                    # Reset counter as connectivity is fine
                    if self._s3_consecutive_errors > 0:
                        logger.info(
                            f"S3 ClientError (404/403) encountered (connectivity restored). "
                            f"Resetting error counter from {self._s3_consecutive_errors} to 0."
                        )
                        self._s3_consecutive_errors = 0
                    raise e

        # If we are here, we are either in FTP mode OR we just switched to FTP mode.
        if self._current_source == SourceType.FTP:
            return self._fetch_ftp(file_path)

        # Should be unreachable
        raise RuntimeError("Unknown source state")

    def _fetch_s3(self, file_path: str) -> bytes:
        """
        Fetches file from S3 using fsspec.
        """
        full_path = f"s3://{self.S3_BUCKET}/{file_path}"
        # cat_file returns bytes
        return self._fs_s3.cat(full_path) # type: ignore

    def _fetch_ftp(self, file_path: str) -> bytes:
        """
        Fetches file from FTP using fsspec.
        """
        # Full path on FTP: /pub/pmc/ + file_path
        full_path = f"{self.FTP_BASE_PATH.rstrip('/')}/{file_path.lstrip('/')}"

        # fsspec FTP cat
        return self._fs_ftp.cat(full_path) # type: ignore

    def close(self) -> None:
        """Cleanup resources."""
        # fsspec filesystems usually manage their own sessions, but we can clear cache
        self._fs_s3.clear_instance_cache()
        self._fs_ftp.clear_instance_cache()
