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

import duckdb

from coreason_etl_pubmedcentral.utils.logger import logger


def parse_manifest(
    manifest_path: str, last_updated_watermark: datetime.datetime | None = None
) -> Generator[dict[str, Any]]:
    """
    AGENT INSTRUCTION: Uses DuckDB to parse the PMC filelist CSV.
    Implements High-Water Mark (Delta) logic by filtering out rows with
    LastUpdated before the watermark.

    Yields dictionaries with the parsed metadata.
    """
    logger.info(f"Parsing manifest: {manifest_path} with watermark: {last_updated_watermark}")

    with duckdb.connect() as con:
        # We use standard SQL read_csv with auto-detection but specify types to be safe
        query = """
            SELECT
                "File" AS file_path,
                "Accession ID" AS accession_id,
                "Last Updated (YYYY-MM-DD HH:MM:SS)" AS last_updated_str,
                "PMID" AS pmid,
                "License" AS license_type,
                "Retracted" AS retracted_str
            FROM read_csv(
                ?,
                header=True,
                auto_detect=True,
                types={'Last Updated (YYYY-MM-DD HH:MM:SS)': 'VARCHAR'}
            )
        """

        args: list[Any] = [manifest_path]

        if last_updated_watermark:
            watermark_str = last_updated_watermark.strftime("%Y-%m-%d %H:%M:%S")
            query += " WHERE try_strptime(\"Last Updated (YYYY-MM-DD HH:MM:SS)\", '%Y-%m-%d %H:%M:%S') > ?"
            args.append(watermark_str)

        try:
            result = con.execute(query, args).fetchall()
        except duckdb.Error as e:
            logger.error(f"DuckDB error executing manifest query: {e}")
            raise

        for row in result:
            file_path, accession_id, last_updated_str, pmid, license_type, retracted_str = row

            is_retracted = False
            # DuckDB might return a boolean if auto_detect inferred it, handle both string and bool
            if isinstance(retracted_str, bool):
                is_retracted = retracted_str
            elif retracted_str and isinstance(retracted_str, str) and retracted_str.strip().lower() == "yes":
                is_retracted = True

            yield {
                "file_path": file_path,
                "accession_id": accession_id,
                "last_updated": last_updated_str,
                "pmid": pmid,
                "license": license_type,
                "retracted": is_retracted,
            }
