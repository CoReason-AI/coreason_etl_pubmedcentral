# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import ftplib
import io
from enum import Enum, auto
from typing import Optional

import boto3
import tenacity
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    ConnectionError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)

from coreason_etl_pubmedcentral.utils.logger import logger


class SourceType(Enum):
    S3 = auto()
    FTP = auto()


class SourceManager:
    """
    Manages fetching files from S3 with a failover to FTP.
    Maintains persistent connections where applicable.
    """

    S3_BUCKET = "pmc-oa-opendata"
    FTP_HOST = "ftp.ncbi.nlm.nih.gov"
    FTP_BASE_PATH = "/pub/pmc/"
    FAILOVER_THRESHOLD = 3

    def __init__(self) -> None:
        self._current_source = SourceType.S3
        self._s3_consecutive_errors = 0

        # S3 Client (Lazy init? No, lightweight enough to init here or on first use)
        # Using unsigned config for public bucket
        self._s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

        # FTP Client state
        self._ftp: Optional[ftplib.FTP] = None

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
            except (
                ConnectionError,
                EndpointConnectionError,
                ConnectTimeoutError,
                ReadTimeoutError,
            ) as e:
                # Only connection/timeout errors trigger failover
                self._s3_consecutive_errors += 1
                logger.warning(
                    f"S3 Connection/Timeout Error ({self._s3_consecutive_errors}/{self.FAILOVER_THRESHOLD}) "
                    f"fetching {file_path}: {e}"
                )

                if self._s3_consecutive_errors >= self.FAILOVER_THRESHOLD:
                    logger.info(
                        f"FailoverEvent — S3 unreachable. Switched to FTP for batch/subsequent requests. "
                        f"Triggered by failure on {file_path}"
                    )
                    self._current_source = SourceType.FTP
                    # Fallthrough to FTP immediately for this request
                else:
                    # Re-raise so dlt knows this file failed (retries handled by dlt or next run)
                    raise e
            except ClientError as e:
                # ClientError (e.g. 404, 403) implies connectivity was successful.
                # Therefore, we reset the connection error counter.
                if self._s3_consecutive_errors > 0:
                    logger.info(
                        f"S3 ClientError encountered (connectivity restored). "
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
        Fetches file from S3.
        """
        # file_path is the key.
        response = self._s3_client.get_object(Bucket=self.S3_BUCKET, Key=file_path)
        return response["Body"].read()  # type: ignore

    def _reconnect_ftp_before_retry(self, retry_state: "tenacity.RetryCallState") -> None:
        """Callback to reconnect FTP before retrying."""
        # Log the exception that caused the retry
        exc = retry_state.outcome.exception()
        logger.warning(f"FTP error fetching file: {exc}. Attempting reconnect.")
        self._close_ftp()
        self._ensure_ftp_connection()

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(2),
        retry=tenacity.retry_if_exception_type(ftplib.all_errors + (EOFError, TimeoutError, OSError)),
        before_sleep=lambda rs: rs.args[0]._reconnect_ftp_before_retry(rs),
        reraise=True,
    )  # type: ignore[misc]
    def _fetch_ftp_retryable(self, full_path: str, bio: io.BytesIO) -> None:
        """
        Fetches file from FTP with automatic retry via tenacity.
        """
        # Ensure buffer is clean for every attempt (prevents corruption on retry after partial write)
        bio.seek(0)
        bio.truncate()

        self._ensure_ftp_connection()
        if not self._ftp:
            raise RuntimeError("FTP connection could not be established.")
        self._ftp.retrbinary(f"RETR {full_path}", bio.write)

    def _fetch_ftp(self, file_path: str) -> bytes:
        """
        Fetches file from FTP, handling connection persistence and reconnection.
        """
        # Full path on FTP: /pub/pmc/ + file_path
        # file_path e.g. "oa_comm/xml/PMC12345.xml"
        # full path: "/pub/pmc/oa_comm/xml/PMC12345.xml"
        full_path = f"{self.FTP_BASE_PATH.rstrip('/')}/{file_path.lstrip('/')}"

        bio = io.BytesIO()
        self._fetch_ftp_retryable(full_path, bio)
        return bio.getvalue()

    def _ensure_ftp_connection(self) -> None:
        """
        Ensures self._ftp is connected.
        """
        if self._ftp:
            # Check if still connected?
            # sending a NOOP is a common way to check.
            try:
                self._ftp.voidcmd("NOOP")
                return
            except ftplib.all_errors + (EOFError,):
                logger.debug("FTP connection lost. Reconnecting...")
                self._close_ftp()

        # Connect
        try:
            self._ftp = ftplib.FTP(self.FTP_HOST)
            self._ftp.login()  # Anonymous login
        except Exception as e:
            logger.error(f"Failed to connect to FTP {self.FTP_HOST}: {e}")
            self._ftp = None
            raise e

    def _close_ftp(self) -> None:
        if self._ftp:
            try:
                self._ftp.quit()
            except ftplib.all_errors + (EOFError,):
                try:
                    self._ftp.close()
                except Exception:
                    pass
            self._ftp = None

    def close(self) -> None:
        """Cleanup resources."""
        self._close_ftp()
