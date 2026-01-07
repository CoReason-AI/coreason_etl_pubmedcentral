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


def parse_manifest(lines: Iterator[str], last_ingested_cutoff: Optional[datetime] = None) -> Iterator[ManifestRecord]:
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
    # Define expected schema columns in order
    fieldnames = [
        "File Path",
        "Accession ID",
        "Last Updated (UTC)",
        "PMID",
        "License",
        "Retracted"
    ]

    # Use DictReader with strict fieldnames to map by position, robust to header content
    # skipinitialspace=True handles whitespace after delimiters
    reader = csv.DictReader(lines, fieldnames=fieldnames, skipinitialspace=True)

    # Skip the header row.
    # Note: lines is an iterator. DictReader wraps it.
    # Calling next(reader) consumes one parsed row.
    try:
        next(reader)
    except StopIteration:
        return

    for row in reader:
        # Check if row is empty/blank
        if not row:
            continue

        # Filter out rows that are entirely None values (blank lines sometimes yield this)
        if all(v is None for v in row.values()):
            continue

        # Extract fields
        # Note: DictReader puts extra fields in 'restkey' and missing fields are None (or restval)

        file_path = row.get("File Path")
        accession_id = row.get("Accession ID")
        last_updated_str = row.get("Last Updated (UTC)")
        pmid_str = row.get("PMID")
        license_type = row.get("License")
        retracted_str = row.get("Retracted")

        # Strip whitespace (DictReader's skipinitialspace handles leading, but we want robust stripping)
        if file_path: file_path = file_path.strip()
        if accession_id: accession_id = accession_id.strip()
        if last_updated_str: last_updated_str = last_updated_str.strip()
        if pmid_str: pmid_str = pmid_str.strip()
        if license_type: license_type = license_type.strip()
        if retracted_str: retracted_str = retracted_str.strip()

        # Validation: Mandatory fields
        if not (file_path and accession_id and last_updated_str):
            continue

        # If Retracted is missing (e.g. short row), default to False?
        # The previous logic checked `len(row) < 6`.
        # If `Retracted` is None, it means the row was short.
        if retracted_str is None:
             # If "Retracted" column is missing, previous code skipped.
             # "if len(row) < 6: continue"
             continue

        # Parsing logic
        is_retracted = retracted_str.lower() == "yes"

        try:
            last_updated = datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S")
            last_updated = last_updated.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue

        if last_ingested_cutoff and last_updated <= last_ingested_cutoff:
            if not is_retracted:
                continue

        pmid = pmid_str if pmid_str else None

        yield ManifestRecord(
            file_path=file_path,
            accession_id=accession_id,
            last_updated=last_updated,
            pmid=pmid,
            license_type=license_type if license_type else "unknown",
            is_retracted=is_retracted,
        )
