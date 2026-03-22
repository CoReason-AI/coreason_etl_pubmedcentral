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
from io import BytesIO

import fsspec
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from coreason_etl_pubmedcentral.config import PubMedCentralConfig
from coreason_etl_pubmedcentral.utils.logger import logger


class EpistemicNetworkClientPolicy(abc.ABC):
    """
    Abstract strategy class defining the interface for network operations.
    Must adhere to Epistemic prefixing.
    """

    @abc.abstractmethod
    def fetch_file(self, file_path: str) -> tuple[BytesIO, str]:
        """
        Fetches the content of a file and returns its payload and source indicator.
        Raises IOError if the fetch fails or the file is zero bytes.
        """


class S3Source(EpistemicNetworkClientPolicy):
    """
    Primary S3 client implementation.
    Accesses the `pmc-oa-opendata` bucket anonymously.
    Transient failures are retried via tenacity.
    """

    def __init__(self, bucket_name: str, max_attempts: int):
        self.bucket_name = bucket_name
        self.fs = fsspec.filesystem("s3", anon=True)
        # Configure tenacity on instance creation using a decorator factory
        self.fetch_file = retry(  # type: ignore[method-assign]
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(OSError),
            reraise=True,
        )(self._fetch_file_impl)

    def fetch_file(self, file_path: str) -> tuple[BytesIO, str]:
        """Implemented dynamically in __init__ using tenacity."""
        raise NotImplementedError("This method is overridden in __init__")

    def _fetch_file_impl(self, file_path: str) -> tuple[BytesIO, str]:
        """
        Underlying logic to fetch from S3.
        """
        full_path = f"s3://{self.bucket_name}/{file_path}"
        try:
            with self.fs.open(full_path, "rb") as f:
                data = f.read()
        except Exception as e:
            logger.warning(f"S3 fetch failed for {full_path}: {e}")
            raise OSError(f"Failed to fetch {full_path} from S3: {e}") from e

        if not data:
            logger.warning(f"S3 returned a zero-byte payload for {full_path}")
            raise OSError(f"Zero-byte file received from S3: {full_path}")

        return BytesIO(data), "S3"


class FTPSource(EpistemicNetworkClientPolicy):
    """
    Secondary FTP client implementation for legacy protocol failover.
    """

    def __init__(self, ftp_host: str, ftp_path: str):
        self.ftp_host = ftp_host
        self.ftp_path = ftp_path
        # Allow FTP via fsspec
        self.fs = fsspec.filesystem("ftp", host=self.ftp_host)

    def fetch_file(self, file_path: str) -> tuple[BytesIO, str]:
        """
        Fetches the file via FTP. We construct the full URL.
        """
        full_path = os.path.join(self.ftp_path, file_path)
        try:
            with self.fs.open(full_path, "rb") as f:
                data = f.read()
        except Exception as e:
            logger.warning(f"FTP fetch failed for {full_path}: {e}")
            raise OSError(f"Failed to fetch {full_path} from FTP: {e}") from e

        if not data:
            logger.warning(f"FTP returned a zero-byte payload for {full_path}")
            raise OSError(f"Zero-byte file received from FTP: {full_path}")

        return BytesIO(data), "FTP"


class EpistemicSourceManagerPolicy:
    """
    Implements a circuit breaker strategy to manage the dual-source architecture.

    AGENT INSTRUCTION:
    This class adheres to the SourceManager resilience rules:
    - S3 is the primary source.
    - An intermittent S3 success resets the error counter to zero.
    - If S3 fails for a specific file, it instantly falls back to FTP to prevent data loss.
    - If S3 fails consecutively matching `s3_max_retry_attempts`, it trips the circuit breaker
      and persistently routes all subsequent requests to FTP for the remaining execution duration.
    """

    def __init__(self, config: PubMedCentralConfig):
        self.s3_source = S3Source(config.s3_bucket, config.s3_max_retry_attempts)
        self.ftp_source = FTPSource(config.ftp_host, config.ftp_path)

        self.max_attempts = config.s3_max_retry_attempts
        self.consecutive_s3_failures = 0
        self.circuit_breaker_tripped = False

    def get_file(self, file_path: str) -> tuple[BytesIO, str]:
        """
        Synchronous method encapsulating logic for data retrieval via the current strategy context.
        """
        if self.circuit_breaker_tripped:
            return self.ftp_source.fetch_file(file_path)

        try:
            result = self.s3_source.fetch_file(file_path)
            # Success resets the counter
            self.consecutive_s3_failures = 0
            return result
        except OSError:
            self.consecutive_s3_failures += 1
            logger.warning(f"S3 failure count incremented to {self.consecutive_s3_failures}")

            if self.consecutive_s3_failures >= self.max_attempts:
                logger.info(
                    "FailoverEvent",
                    source="s3",
                    status="fail",
                    message="S3 unreachable. Switched to FTP for remaining batch.",
                )
                self.circuit_breaker_tripped = True

            # Instantly fallback to FTP for this specific file
            logger.info("FailoverEvent", file_path=file_path, message="Falling back to FTP for specific file.")
            return self.ftp_source.fetch_file(file_path)
