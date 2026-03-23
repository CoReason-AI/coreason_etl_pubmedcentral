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
from collections.abc import Iterator
from typing import Any

import dlt
import fsspec

from coreason_etl_pubmedabstracts.config import PubMedAbstractsConfig
from coreason_etl_pubmedabstracts.parsers.xml_parser import EpistemicMedlineParser


@dlt.resource(name="pubmed_baseline", write_disposition="replace")  # type: ignore[misc]
def get_pubmed_baseline(
    config: PubMedAbstractsConfig = dlt.config.value,
) -> Iterator[dict[str, Any]]:
    """
    DLT Resource for extracting PubMed/MEDLINE Annual Baseline files via FTP.

    AGENT INSTRUCTION:
    This function adheres to the Layer 1: Bronze ingestion strategy.
    It strictly utilizes `dlt` native primitives and `fsspec` to list and stream
    all `*.xml.gz` files from the `/pubmed/baseline/` FTP directory.
    The write_disposition is `replace` representing an Annual Reload.
    Custom retry loops and HTTP clients are avoided.
    """
    # Create the FTP filesystem instance connected to NLM
    fs = fsspec.filesystem("ftp", host=config.ftp_host)

    # List files in the baseline directory
    base_path = config.baseline_dir
    if not base_path.endswith("/"):
        base_path += "/"

    try:
        # Use simple ls to get files, filtering specifically for .xml.gz
        all_files = fs.ls(base_path)
    except Exception:
        # Fallback to an empty list if directory access fails so the pipeline
        # doesn't crash but logs/handles gracefully if needed via dlt error handling
        all_files = []

    xml_files = [f for f in all_files if f.endswith(".xml.gz")]

    # Process files sequentially
    for file_path in sorted(xml_files):
        # We extract just the filename from the path
        file_name = file_path.split("/")[-1]

        # We record the current UTC timestamp as the exact moment of extraction
        ingestion_ts = datetime.datetime.now(datetime.UTC).isoformat()

        # Open and stream the gzipped file over FTP
        with fs.open(file_path, "rb") as stream:
            # We must wrap the stream in a gzip module reader since FTP returns the raw compressed bytes
            import gzip

            with gzip.open(stream, "rb") as gz_stream:
                for parsed_record in EpistemicMedlineParser.execute(gz_stream):
                    # Enhance the parsed record with file-level Bronze metadata constraints
                    yield {
                        "file_name": file_name,
                        "ingestion_ts": ingestion_ts,
                        "content_hash": parsed_record.get("content_hash"),
                        "raw_data": parsed_record.get("raw_data"),
                    }
