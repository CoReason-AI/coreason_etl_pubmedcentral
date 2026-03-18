# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import csv
import datetime
from collections.abc import Generator
from typing import Any

from coreason_etl_pubmedcentral.utils.logger import logger


def parse_manifest(
    manifest_path: str, last_updated_watermark: datetime.datetime | None = None
) -> Generator[dict[str, Any]]:
    """
    AGENT INSTRUCTION: Uses standard Python to parse the PMC filelist CSV.
    Implements High-Water Mark (Delta) logic by filtering out rows with
    LastUpdated before the watermark.

    Yields dictionaries with the parsed metadata.
    """
    logger.info(f"Parsing manifest: {manifest_path} with watermark: {last_updated_watermark}")

    try:
        with open(manifest_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                file_path = row.get("File", "")
                accession_id = row.get("Accession ID", "")
                last_updated_str = row.get("Last Updated (YYYY-MM-DD HH:MM:SS)", "")
                pmid_str = row.get("PMID", "")
                license_type = row.get("License", "")
                retracted_str = row.get("Retracted", "")

                # High-Water Mark (Delta) Logic
                if last_updated_watermark and last_updated_str:
                    try:
                        row_date = datetime.datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S")
                        if row_date <= last_updated_watermark:
                            continue
                    except ValueError:
                        # If date parsing fails, skip the row based on robust date handling requirement
                        logger.warning(f"Failed to parse date for {accession_id}: {last_updated_str}")
                        continue

                # Handle retracted status
                is_retracted = False
                if retracted_str and isinstance(retracted_str, str) and retracted_str.strip().lower() == "yes":
                    is_retracted = True

                # Handle PMID string parsing explicitly
                pmid = pmid_str.strip() if pmid_str and pmid_str.strip() else None

                yield {
                    "file_path": file_path,
                    "accession_id": accession_id,
                    "last_updated": last_updated_str,
                    "pmid": pmid,
                    "license": license_type,
                    "retracted": is_retracted,
                }
    except Exception as e:
        logger.error(f"Error parsing manifest file: {e}")
        raise
