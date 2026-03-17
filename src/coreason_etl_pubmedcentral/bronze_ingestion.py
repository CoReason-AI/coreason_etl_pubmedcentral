# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import datetime
from collections.abc import Generator
from typing import Any

import dlt

from coreason_etl_pubmedcentral.config import PubMedCentralConfiguration
from coreason_etl_pubmedcentral.manifest_parser import parse_manifest
from coreason_etl_pubmedcentral.source_manager import SourceManager
from coreason_etl_pubmedcentral.utils.logger import logger


def _pmc_xml_files_generator(
    config: PubMedCentralConfiguration, manifest_path: str, last_updated: dlt.sources.incremental[datetime.datetime]
) -> Generator[dict[str, Any]]:
    """
    AGENT INSTRUCTION: Internal generator logic for the bronze resource.
    Refactored to separate the internal generator logic from the decorated function
    to support unit testing, as dlt resources cannot easily be tested directly.
    """
    source_manager = SourceManager(s3_bucket=config.s3_bucket, ftp_host=config.ftp_host, ftp_path=config.ftp_path)

    # Get the last value of the watermark, or None if it's the first run
    watermark_value = last_updated.last_value

    logger.info(f"Bronze ingestion started with watermark: {watermark_value}")

    records = parse_manifest(manifest_path, last_updated_watermark=watermark_value)

    for record in records:
        file_path = record["file_path"]

        # Try to download the file. If it fails, log and skip (or we can let the error bubble up,
        # but the spec says single-file failures fallback to FTP, so it shouldn't fail completely
        # unless both S3 and FTP fail).
        try:
            local_path = source_manager.get_file(file_path)
            # The ingestion_source should be FTP if the failover is active or if the file was fetched from FTP
            ingestion_source = "FTP" if source_manager.is_failover_active else "S3"

            # In a real scenario, we might want SourceManager to return a tuple (path, source).
            # For simplicity and given the memory constraints, this is sufficient.
            logger.info("records_ingested_total", source=ingestion_source, status="success")

        except Exception as e:
            failed_source = "FTP" if source_manager.is_failover_active else "S3"
            logger.error(f"Failed to fetch file {file_path}: {e}")
            logger.info("records_ingested_total", source=failed_source, status="fail")
            continue

        # Validate that local_path and ingestion_source are strings
        if not isinstance(local_path, str):
            raise TypeError("Expected source_file_path to be a string")
        if not isinstance(ingestion_source, str):
            raise TypeError("Expected ingestion_source to be a string")  # pragma: no cover

        if record.get("retracted"):
            logger.info(f"RetractionFound - Marking {record['accession_id']} as retracted based on file.")

        file_metadata = {
            "accession_id": record["accession_id"],
            "last_updated": record["last_updated"],
            "pmid": record["pmid"],
            "license": record["license"],
            "retracted": record["retracted"],
            "original_file_path": file_path,
        }

        # Parse last_updated to datetime for High-Water Mark tracking
        # The manifest format is "YYYY-MM-DD HH:MM:SS"
        try:
            record_last_updated = datetime.datetime.strptime(record["last_updated"], "%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            logger.error(f"Failed to parse last_updated date: {record['last_updated']} - {e}")
            continue

        yield {
            "source_file_path": local_path,
            "ingestion_ts": datetime.datetime.now(datetime.UTC),
            "ingestion_source": ingestion_source,
            "file_metadata": file_metadata,
            "last_updated": record_last_updated,  # Include this for dlt incremental tracking
        }


# Ruff complains about B008 with dlt.sources.incremental("last_updated"), but this is the idiomatic dlt way.
# We ignore B008 for this specific line.
@dlt.resource(name="bronze_pmc_file", write_disposition="append")
def pmc_xml_files(
    config: PubMedCentralConfiguration = dlt.config.value,
    manifest_path: str = dlt.config.value,
    last_updated: dlt.sources.incremental[datetime.datetime] = dlt.sources.incremental("last_updated"),  # noqa: B008
) -> Generator[dict[str, Any]]:
    """
    AGENT INSTRUCTION: Bronze layer dlt resource.
    Yields records containing the local file path and metadata.
    Does not store XML directly in PostgreSQL.
    """
    yield from _pmc_xml_files_generator(
        config=config, manifest_path=manifest_path, last_updated=last_updated
    )  # pragma: no cover
