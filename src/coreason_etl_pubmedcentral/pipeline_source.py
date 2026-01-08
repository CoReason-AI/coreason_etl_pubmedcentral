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

from coreason_etl_pubmedcentral.manifest import parse_manifest
from coreason_etl_pubmedcentral.source_manager import SourceManager
from coreason_etl_pubmedcentral.utils.logger import logger


def _pmc_xml_files_generator(
    manifest_file_path: str,
    remote_manifest_path: Optional[str],
    source_manager: SourceManager,
    last_updated: dlt.sources.incremental[Any],
) -> Iterator[dict[str, Any]]:
    """
    Internal generator logic for pmc_xml_files.
    Separated for easier unit testing without dlt decorator interference.
    """
    if remote_manifest_path:
        logger.info(f"Downloading manifest from {remote_manifest_path} to {manifest_file_path}")
        try:
            content = source_manager.get_file(remote_manifest_path)
            with open(manifest_file_path, "wb") as f:
                f.write(content)
        except Exception:
            logger.exception(f"Failed to download manifest from {remote_manifest_path}")
            raise

    # Determine cutoff from incremental state
    last_ingested_cutoff: Optional[datetime] = None
    if last_updated.start_value:
        try:
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
        with open(manifest_file_path, "r", encoding="utf-8-sig") as f:
            for record in parse_manifest(f, last_ingested_cutoff=last_ingested_cutoff):
                context_logger = logger.bind(file_path=record.file_path)
                try:
                    content_bytes = source_manager.get_file(record.file_path)

                    manifest_metadata = asdict(record)
                    manifest_metadata["last_updated"] = record.last_updated.isoformat()

                    current_ts = datetime.now(timezone.utc)
                    source_name = source_manager._current_source.name

                    # Bronze Validation: Validate VARCHAR data types explicitly
                    if not isinstance(record.file_path, str):
                        raise TypeError(
                            f"Validation Error: source_file_path must be a string, got {type(record.file_path)}"
                        )
                    if not isinstance(source_name, str):
                        raise TypeError(f"Validation Error: ingestion_source must be a string, got {type(source_name)}")

                    yield {
                        "source_file_path": record.file_path,
                        "ingestion_ts": current_ts,
                        "ingestion_date": current_ts.date(),
                        "ingestion_source": source_name,
                        "raw_xml_payload": content_bytes,
                        "manifest_metadata": manifest_metadata,
                        "last_updated": record.last_updated,
                    }

                    context_logger.bind(
                        metric="records_ingested_total",
                        labels={
                            "source": source_manager._current_source.name.lower(),
                            "status": "success",
                        },
                    ).info("Metric: records_ingested_total")

                except TypeError as e:
                    # Explicitly re-raise validation errors so they aren't swallowed by the generic Exception catch
                    raise e
                except Exception:
                    context_logger.exception(f"Failed to ingest file {record.file_path}")
                    context_logger.bind(
                        metric="records_ingested_total",
                        labels={
                            "source": source_manager._current_source.name.lower(),
                            "status": "fail",
                        },
                    ).info("Metric: records_ingested_total")
                    pass

    except FileNotFoundError:
        logger.error(f"Manifest file not found: {manifest_file_path}")
        raise


@dlt.source  # type: ignore[misc]
def pmc_source(
    manifest_file_path: str,
    remote_manifest_path: Optional[str] = None,
    source_manager: Optional[SourceManager] = None,
) -> Any:
    """
    DLT Source for PubMed Central Open Access subset.
    Orchestrates the ingestion of XML files based on the manifest.
    """
    return pmc_xml_files(manifest_file_path, remote_manifest_path, source_manager)


@dlt.resource(
    write_disposition="append",
    columns={
        "ingestion_date": {"data_type": "date", "partition": True},
        "source_file_path": {"data_type": "text"},
        "ingestion_ts": {"data_type": "timestamp"},
        "ingestion_source": {"data_type": "text"},
        "raw_xml_payload": {"data_type": "binary"},
        "manifest_metadata": {"data_type": "json"},
        "last_updated": {"data_type": "timestamp"},
    },
)  # type: ignore[misc]
def pmc_xml_files(
    manifest_file_path: str,
    remote_manifest_path: Optional[str] = None,
    source_manager: Optional[SourceManager] = None,
    last_updated: dlt.sources.incremental[Any] = dlt.sources.incremental("last_updated"),  # noqa: B008
) -> Iterator[dict[str, Any]]:
    """
    Resource that yields XML content and metadata for PMC articles.
    """
    if source_manager is None:
        source_manager = SourceManager()

    yield from _pmc_xml_files_generator(manifest_file_path, remote_manifest_path, source_manager, last_updated)
