# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import os
import tarfile
import tempfile
from collections.abc import Generator
from unittest import mock

import pytest

from coreason_etl_pubmedcentral.main import run_pipeline


@pytest.fixture
def mock_tarball() -> Generator[str]:
    """Creates a valid dummy tarball containing a mock PMC XML."""
    fd, tar_path = tempfile.mkstemp(suffix=".tar.gz")
    os.close(fd)

    xml_content = b"""<?xml version="1.0" ?>
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">12345</article-id>
                <pub-date pub-type="epub">
                    <year>2024</year>
                    <month>01</month>
                    <day>01</day>
                </pub-date>
            </article-meta>
        </front>
    </article>
    """

    # Create temp xml file
    fd_xml, xml_path = tempfile.mkstemp(suffix=".nxml")
    with os.fdopen(fd_xml, "wb") as f:
        f.write(xml_content)

    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(xml_path, arcname="PMC12345.nxml")

    os.remove(xml_path)

    yield tar_path

    if os.path.exists(tar_path):
        os.remove(tar_path)


@pytest.mark.live
@mock.patch("coreason_etl_pubmedcentral.bronze_ingestion.SourceManager")
def test_live_pipeline_execution(mock_sm_class: mock.MagicMock, mock_tarball: str) -> None:
    """
    Integration test for the full Medallion pipeline.
    Mocks the SourceManager to return a locally generated tarball, avoiding real network calls
    and handling the flat .xml reality of the S3 bucket.
    Executes the pipeline using the "dummy" destination to verify in-memory extraction
    without needing a real PostgreSQL connection.
    """
    import os

    import dlt

    # Mock the SourceManager to return the mock_tarball path
    mock_sm_instance = mock_sm_class.return_value
    mock_sm_instance.get_file.return_value = mock_tarball
    mock_sm_instance.is_failover_active = False

    # Create a small slice of the real manifest
    fd, manifest_path = tempfile.mkstemp(suffix=".csv")
    content = "File,Article Citation,Accession ID,Last Updated (YYYY-MM-DD HH:MM:SS),PMID,License,Retracted\n"
    content += "oa_comm/xml/all/PMC12345.xml.tar.gz,Cite,PMC12345,2024-01-01 12:00:00,12345,CC-BY,no\n"

    with os.fdopen(fd, "w") as f:
        f.write(content)

    # Force dlt to use the dummy destination using environment variables
    # dlt will pick up the destination config from the environment
    os.environ["DESTINATION__NAME"] = "dummy"
    os.environ["PMC_PIPELINE__DESTINATION"] = "dummy"

    # We also mock dlt.pipeline locally to force destination="dummy"
    # because main.py hardcodes destination="postgres"
    original_pipeline = dlt.pipeline

    from typing import Any

    def mock_pipeline(*args: Any, **kwargs: Any) -> Any:
        from dlt.destinations import dummy

        kwargs["destination"] = dummy(completed_prob=1.0)
        return original_pipeline(*args, **kwargs)

    with mock.patch("coreason_etl_pubmedcentral.main.dlt.pipeline", side_effect=mock_pipeline):
        try:
            run_pipeline(manifest_path)
        finally:
            os.remove(manifest_path)
            os.environ.pop("DESTINATION__NAME", None)
            os.environ.pop("PMC_PIPELINE__DESTINATION", None)
