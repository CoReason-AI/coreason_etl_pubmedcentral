# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason-etl-pubmedabstracts

from collections.abc import Iterator
from typing import Any

import dlt

from coreason_etl_pubmedabstracts.config import PubMedAbstractsConfig
from coreason_etl_pubmedabstracts.utils.ftp_streamer import EpistemicFTPStreamingPolicy


@dlt.resource(name="pubmed_baseline", write_disposition="replace")  # type: ignore[misc]
def get_pubmed_baseline(
    config: PubMedAbstractsConfig = dlt.config.value,
) -> Iterator[dict[str, Any]]:
    """
    DLT Resource for extracting PubMed/MEDLINE Annual Baseline files via FTP.

    AGENT INSTRUCTION:
    This function adheres to the Layer 1: Bronze ingestion strategy.
    It utilizes the shared EpistemicFTPStreamingPolicy to list and stream all `*.xml.gz` files
    from the `/pubmed/baseline/` FTP directory.
    The write_disposition is `replace` representing an Annual Reload.
    Custom retry loops and HTTP clients are avoided.
    """
    yield from EpistemicFTPStreamingPolicy.execute(
        ftp_host=config.ftp_host,
        directory_path=config.baseline_dir,
        sort_alphanumeric=True,
    )
