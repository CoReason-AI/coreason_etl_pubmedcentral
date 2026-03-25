# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason-etl-pubmedabstracts

import datetime
import gzip
from collections.abc import Iterator
from typing import Any
import io
import fsspec

from coreason_etl_pubmedabstracts.parsers.xml_parser import EpistemicMedlineParser
from coreason_etl_pubmedcentral.utils.logger import logger


class EpistemicFTPStreamingPolicy:
    """
    Consolidated policy for extracting and streaming compressed XML files over FTP.

    AGENT INSTRUCTION:
    This class adheres to the DRY principle by centralizing the `fsspec` connection logic,
    directory listing, `.xml.gz` filtering, and parsing delegation across both Baseline and Updates.
    It catches explicit OSErrors during listing rather than masking them with a generic Exception.
    """

    @classmethod
    def execute(
        cls, ftp_host: str, directory_path: str, *, sort_alphanumeric: bool = False
    ) -> Iterator[dict[str, Any]]:
        """
        Streams `*.xml.gz` files from an FTP directory and yields parsed records.

        Args:
            ftp_host: The domain/IP of the FTP server.
            directory_path: The remote path (e.g., '/pubmed/baseline/').
            sort_alphanumeric: If True, strictly sorts filenames to guarantee sequential delta application.

        Yields:
            A dictionary containing parsed metadata attributes and the raw XML payload.
        """
        fs = fsspec.filesystem("ftp", host=ftp_host)
        base_path = directory_path if directory_path.endswith("/") else f"{directory_path}/"

        try:
            all_files = fs.ls(base_path, detail=False)
        except Exception as e:
            # Catch file system connection/listing errors rather than generic Exception
            logger.warning(f"FTP directory listing failed for {base_path}: {e}")
            all_files = []

        # Filter to target files only
        xml_files = [f for f in all_files if f.endswith(".xml.gz")]

        if sort_alphanumeric:
            xml_files.sort()

        for file_path in xml_files:
            file_name = file_path.split("/")[-1]
            # Force the absolute path to guarantee we don't query the FTP root
            full_path = f"{base_path}{file_name}"
            ingestion_ts = datetime.datetime.now(datetime.UTC).isoformat()
            
            try:
                # 1. Read the compressed payload entirely into memory to close the FTP transfer quickly
                with fs.open(full_path, "rb") as stream:
                    compressed_payload = stream.read()

                # 2. Wrap the payload in a BytesIO buffer and stream it to the local parser
                with gzip.open(io.BytesIO(compressed_payload), "rb") as gz_stream:
                    for parsed_record in EpistemicMedlineParser.execute(gz_stream):
                        yield {
                            "file_name": file_name,
                            "ingestion_ts": ingestion_ts,
                            "content_hash": parsed_record.get("content_hash"),
                            "raw_data": parsed_record.get("raw_data"),
                        }
            except Exception as e:
                # Log fetch failures per file
                logger.error(f"Failed to fetch or parse file {full_path}: {e}")
                continue

