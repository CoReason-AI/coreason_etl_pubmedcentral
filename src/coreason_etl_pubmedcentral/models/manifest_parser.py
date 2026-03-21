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
from collections.abc import Iterator
from typing import Any


class EpistemicManifestParsingTask:
    """
    EpistemicManifestParsingTask handles the transformation of a PMC filelist CSV
    into an enumerable stream of validated dictionaries representing source files.

    AGENT INSTRUCTION:
    This class must enforce the High-Water Mark (Delta) strategy. It strictly
    ignores files modified at or before the `last_processed_ts` parameter
    to guarantee exactly-once processing semantic boundaries.
    """

    @classmethod
    def execute(
        cls, file_stream: Iterator[str], last_processed_ts: datetime.datetime | None = None
    ) -> Iterator[dict[str, Any]]:
        """
        Transmute a raw CSV text stream into a filtered metadata manifest.

        Args:
            file_stream: An iterable sequence of strings (e.g., an open file handle)
                         containing the CSV data.
            last_processed_ts: A UTC datetime representing the maximum `Last Updated (UTC)`
                               timestamp of successfully processed records.

        Yields:
            A dictionary containing parsed metadata attributes for each file that exceeds
            the High-Water Mark.
        """
        # The PMC manifest uses a specific column layout.
        # Example: File Path,Accession ID,Last Updated (UTC),PMID,License,Retracted
        reader = csv.DictReader(file_stream)

        if reader.fieldnames is None:
            return

        for row in reader:
            # Safely extract raw values
            file_path = row.get("File Path", "").strip()
            accession_id = row.get("Accession ID", "").strip()
            last_updated_str = row.get("Last Updated (UTC)", "").strip()
            pmid = row.get("PMID", "").strip()
            license_type = row.get("License", "").strip()
            retracted_str = row.get("Retracted", "").strip().lower()

            if not file_path or not last_updated_str:
                continue

            try:
                # Format: 2024-03-25 15:42:01
                updated_ts = datetime.datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S")
                # Ensure the parsed timestamp is timezone-aware (UTC)
                updated_ts = updated_ts.replace(tzinfo=datetime.UTC)
            except ValueError:
                # If parsing fails, skip the malformed row to prevent pipeline halting.
                continue

            # Delta Strategy Boundary Condition
            if last_processed_ts is not None and updated_ts <= last_processed_ts:
                continue

            # Retraction Boolean Mapping
            is_retracted = retracted_str == "yes"

            yield {
                "file_path": file_path,
                "accession_id": accession_id,
                "last_updated": updated_ts,
                "pmid": pmid or None,
                "license": license_type,
                "is_retracted": is_retracted,
            }
