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

import dlt
import fsspec

from coreason_etl_pubmedabstracts.config import PubMedAbstractsConfig
from coreason_etl_pubmedabstracts.parsers.xml_parser import EpistemicMedlineParser


@dlt.resource(name="pubmed_updates", write_disposition="append")  # type: ignore[misc]
def get_pubmed_updates(
    config: PubMedAbstractsConfig = dlt.config.value,
) -> Iterator[dict[str, Any]]:
    """
    DLT Resource for extracting PubMed/MEDLINE Daily Update files via FTP.

    AGENT INSTRUCTION:
    This function adheres strictly to the Layer 1: Bronze ingestion strategy.
    It lists `*.xml.gz` files from the `/pubmed/updatefiles/` FTP directory,
    enforcing alphanumeric sorting to guarantee sequential application of Delta updates.
    The write_disposition is `append` for continuous daily loads.
    """
    fs = fsspec.filesystem("ftp", host=config.ftp_host)

    base_path = config.updates_dir
    if not base_path.endswith("/"):
        base_path += "/"

    try:
        all_files = fs.ls(base_path)
    except Exception:
        # Fallback to an empty list gracefully
        all_files = []

    xml_files = [f for f in all_files if f.endswith(".xml.gz")]

    # Delta Logic Constraint: Strict alphanumeric sorting to ensure sequential application
    for file_path in sorted(xml_files):
        file_name = file_path.split("/")[-1]
        ingestion_ts = datetime.datetime.now(datetime.UTC).isoformat()

        with fs.open(file_path, "rb") as stream, gzip.open(stream, "rb") as gz_stream:
            for parsed_record in EpistemicMedlineParser.execute(gz_stream):
                yield {
                    "file_name": file_name,
                    "ingestion_ts": ingestion_ts,
                    "content_hash": parsed_record.get("content_hash"),
                    "raw_data": parsed_record.get("raw_data"),
                }
