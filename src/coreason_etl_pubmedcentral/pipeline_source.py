# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from collections.abc import Iterator
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Optional

import dlt
from loguru import logger

from coreason_etl_pubmedcentral.manifest import parse_manifest
from coreason_etl_pubmedcentral.source_manager import SourceManager


@dlt.source  # type: ignore[misc]
def pmc_source(
    manifest_file_path: str,
    source_manager: Optional[SourceManager] = None,
) -> Any:
    """
    DLT Source for PubMed Central Open Access subset.

    Args:
        manifest_file_path: Path to the local CSV manifest file.
        source_manager: Instance of SourceManager. If None, one will be created.

    Returns:
        The dlt resources.
    """
    return pmc_xml_files(manifest_file_path, source_manager)


@dlt.resource(write_disposition="append")  # type: ignore[misc]
def pmc_xml_files(
    manifest_file_path: str,
    source_manager: Optional[SourceManager] = None,
    last_updated: dlt.sources.incremental[Any] = dlt.sources.incremental("last_updated"),  # noqa: B008
) -> Iterator[dict[str, Any]]:
    """
    Resource that yields XML content and metadata for PMC articles.
    """
    if source_manager is None:
        source_manager = SourceManager()

    # Determine cutoff from incremental state
    last_ingested_cutoff: Optional[datetime] = None
    if last_updated.start_value:
        try:
            # dlt stores state as ISO string
            # If start_value is already a datetime (e.g. from state), use it.
            # If it's a string, parse it.
            if isinstance(last_updated.start_value, str):
                last_ingested_cutoff = datetime.fromisoformat(last_updated.start_value)
            elif isinstance(last_updated.start_value, datetime):
                last_ingested_cutoff = last_updated.start_value

            if last_ingested_cutoff and last_ingested_cutoff.tzinfo is None:
                last_ingested_cutoff = last_ingested_cutoff.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            logger.warning(f"Could not parse incremental start value: {last_updated.start_value}")
            last_ingested_cutoff = None

    logger.info(f"Starting ingestion with cutoff: {last_ingested_cutoff}")

    try:
        with open(manifest_file_path, "r", encoding="utf-8") as f:
            for record in parse_manifest(f, last_ingested_cutoff=last_ingested_cutoff):
                try:
                    # Fetch file content
                    content_bytes = source_manager.get_file(record.file_path)

                    # Decode to string (assuming UTF-8)
                    raw_xml = content_bytes.decode("utf-8")

                    # Metadata
                    manifest_metadata = asdict(record)
                    # Convert datetime objects to ISO strings for JSON serialization compatibility
                    manifest_metadata["last_updated"] = record.last_updated.isoformat()

                    yield {
                        "source_file_path": record.file_path,
                        "ingestion_ts": datetime.now(timezone.utc),
                        "ingestion_source": source_manager._current_source.name,
                        "raw_xml_payload": raw_xml,
                        "manifest_metadata": manifest_metadata,
                        "last_updated": record.last_updated,
                    }

                except Exception as e:
                    logger.error(f"Failed to ingest file {record.file_path}: {e}")
                    pass

    except FileNotFoundError:
        logger.error(f"Manifest file not found: {manifest_file_path}")
        raise
