# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator, Optional


@dataclass(frozen=True)
class ManifestRecord:
    file_path: str
    accession_id: str
    last_updated: datetime
    pmid: Optional[str]
    license_type: str
    is_retracted: bool


def parse_manifest(
    lines: Iterator[str], last_ingested_cutoff: Optional[datetime] = None
) -> Iterator[ManifestRecord]:
    """
    Parses the PMC OA filelist CSV and yields ManifestRecord objects.

    Schema: File Path | Accession ID | Last Updated (UTC) | PMID | License | Retracted
    Example: oa_comm/xml/PMC1.xml,PMC1,2024-01-01 12:00:00,1234,CC-BY,no

    Args:
        lines: An iterator over the lines of the CSV file.
        last_ingested_cutoff: If provided, only yield records updated AFTER this timestamp.

    Yields:
        ManifestRecord objects.
    """
    reader = csv.reader(lines)

    # Attempt to skip header if it exists
    # The spec doesn't explicitly say there is a header, but typically there is.
    # We can inspect the first row. If parsing fails, we might assume it was a header.
    # Or strict adherence: The schema lists "Column Name" in the spec, implying a header.
    # We'll assume the first row is a header.
    try:
        header = next(reader)
    except StopIteration:
        return

    # Check if first row is actually data or header.
    # "File Path" or "oa_comm/..."
    # If it looks like a header, proceed. If not, treat as data?
    # Standard CSVs usually have headers. We will assume header presence.
    # If the first column of header is not "File Path" (or similar), we might log a warning?
    # For now, blindly skip first row.

    for row_idx, row in enumerate(reader, start=2):
        if not row:
            continue

        if len(row) < 6:
            # Log warning or skip?
            # For strictness, we might skip.
            continue

        # Extract fields
        # 0: File Path
        # 1: Accession ID
        # 2: Last Updated (UTC)
        # 3: PMID
        # 4: License
        # 5: Retracted
        file_path = row[0].strip()
        accession_id = row[1].strip()
        last_updated_str = row[2].strip()
        pmid_str = row[3].strip()
        license_type = row[4].strip()
        retracted_str = row[5].strip().lower()

        # Parse Timestamp
        # Format assumed: YYYY-MM-DD HH:MM:SS (UTC)
        # We need to handle potential format variations if any, but start strict.
        try:
            # We assume UTC.
            last_updated = datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S")
            last_updated = last_updated.replace(tzinfo=timezone.utc)
        except ValueError:
            # Fallback or error?
            # If date is unparseable, we cannot determine cutoff. Skip or error.
            # We skip invalid rows to avoid crashing the whole pipeline.
            continue

        # Filter by High Water Mark
        if last_ingested_cutoff and last_updated <= last_ingested_cutoff:
            continue

        # Parse PMID (Nullable)
        pmid = pmid_str if pmid_str else None

        # Parse Retracted
        is_retracted = retracted_str == "yes"

        yield ManifestRecord(
            file_path=file_path,
            accession_id=accession_id,
            last_updated=last_updated,
            pmid=pmid,
            license_type=license_type,
            is_retracted=is_retracted,
        )
