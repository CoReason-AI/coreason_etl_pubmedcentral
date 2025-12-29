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
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectionError
from loguru import logger


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
            except (ClientError, ConnectionError, Exception) as e:
                # We catch generic Exception too because boto3 can raise various errors
                # (e.g. EndpointConnectionError)
                self._s3_consecutive_errors += 1
                logger.warning(
                    f"S3 Error ({self._s3_consecutive_errors}/{self.FAILOVER_THRESHOLD}) fetching {file_path}: {e}"
                )

                if self._s3_consecutive_errors >= self.FAILOVER_THRESHOLD:
                    logger.info(
                        f"S3 unreachable. Switched to FTP for batch/subsequent requests. "
                        f"Triggered by failure on {file_path}"
                    )
                    self._current_source = SourceType.FTP
                    # Fallthrough to FTP immediately for this request
                else:
                    # If we haven't switched yet, we re-raise the error for this file
                    # OR we could try to return None? The interface says bytes.
                    # The spec says "Automatic failover initiated after 3...".
                    # Implicitly, the current file failing might be lost if we don't handle it.
                    # But if we haven't failed over, we assume S3 is still primary.
                    # Retrying this specific file on S3 is handled by caller (dlt) usually.
                    # However, to facilitate the specific "failover" behavior,
                    # we should probably try FTP if we JUST switched.
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

    def _fetch_ftp(self, file_path: str) -> bytes:
        """
        Fetches file from FTP, handling connection persistence and reconnection.
        """
        self._ensure_ftp_connection()
        if not self._ftp:
            raise RuntimeError("FTP connection could not be established.")

        # Full path on FTP: /pub/pmc/ + file_path
        # file_path e.g. "oa_comm/xml/PMC12345.xml"
        # full path: "/pub/pmc/oa_comm/xml/PMC12345.xml"
        # We need to be careful about slashes.
        full_path = f"{self.FTP_BASE_PATH.rstrip('/')}/{file_path.lstrip('/')}"

        bio = io.BytesIO()
        try:
            self._ftp.retrbinary(f"RETR {full_path}", bio.write)
        except ftplib.all_errors + (EOFError,) as e:
            logger.warning(f"FTP error fetching {full_path}: {e}. Attempting reconnect.")
            # Try to reconnect once
            self._close_ftp()
            self._ensure_ftp_connection()
            if not self._ftp:
                raise e
            # Retry fetch
            bio = io.BytesIO()  # Reset buffer
            self._ftp.retrbinary(f"RETR {full_path}", bio.write)

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
