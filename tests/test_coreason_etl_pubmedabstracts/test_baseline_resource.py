# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason-etl-pubmedabstracts

import gzip
import io
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from coreason_etl_pubmedabstracts.config import PubMedAbstractsConfig
from coreason_etl_pubmedabstracts.resources.baseline import get_pubmed_baseline


@pytest.fixture
def test_config() -> PubMedAbstractsConfig:
    """Fixture providing a baseline configuration."""
    config = PubMedAbstractsConfig()
    config.ftp_host = "ftp.mock.org"
    config.baseline_dir = "/mock/baseline/"
    return config


def test_get_pubmed_baseline_success(test_config: PubMedAbstractsConfig) -> None:
    """Test the successful extraction of files from FTP."""
    # Create a mock GZipped XML file
    xml_data = b"""<?xml version="1.0" ?>
    <PubmedArticleSet>
        <PubmedArticle>
            <MedlineCitation Status="MEDLINE">
                <PMID>123</PMID>
                <Article><ArticleTitle>Test</ArticleTitle></Article>
            </MedlineCitation>
        </PubmedArticle>
    </PubmedArticleSet>
    """
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="w") as f:
        f.write(xml_data)

    mock_gz_stream = io.BytesIO(out.getvalue())

    # Mock fsspec filesystem
    mock_fs = MagicMock()
    # Return two fake xml.gz files, one txt file (should be ignored)
    mock_fs.ls.return_value = [
        "/mock/baseline/pubmed26n0001.xml.gz",
        "/mock/baseline/pubmed26n0002.xml.gz",
        "/mock/baseline/readme.txt",
    ]

    # Mock open to return our GZ stream. For simplicity, we return the same stream.
    # We must reset stream position if called multiple times, so use a side effect
    def mock_open_side_effect(*_args: Any, **_kwargs: Any) -> MagicMock:
        mock_gz_stream.seek(0)
        return MagicMock(__enter__=lambda _: mock_gz_stream, __exit__=lambda *_x: None)

    mock_fs.open.side_effect = mock_open_side_effect

    with patch("fsspec.filesystem", return_value=mock_fs):
        # Instantiate the resource
        resource_generator = get_pubmed_baseline(config=test_config)

        # Execute the generator
        results = list(resource_generator)

        # We expect 2 items (1 record per xml.gz file)
        assert len(results) == 2

        # Check first item schema
        assert results[0]["file_name"] == "pubmed26n0001.xml.gz"
        assert "ingestion_ts" in results[0]
        assert "content_hash" in results[0]
        assert results[0]["raw_data"]["PMID"] == "123"

        # Check second item schema
        assert results[1]["file_name"] == "pubmed26n0002.xml.gz"


def test_get_pubmed_baseline_fs_failure(test_config: PubMedAbstractsConfig) -> None:
    """Test graceful handling of FTP ls failure."""
    mock_fs = MagicMock()
    mock_fs.ls.side_effect = Exception("FTP Connection Refused")

    with patch("fsspec.filesystem", return_value=mock_fs):
        resource_generator = get_pubmed_baseline(config=test_config)
        results = list(resource_generator)

        # Should gracefully return empty list
        assert len(results) == 0


def test_get_pubmed_baseline_no_trailing_slash(test_config: PubMedAbstractsConfig) -> None:
    """Test adding trailing slash to directory paths."""
    test_config.baseline_dir = "/mock/no_slash"

    mock_fs = MagicMock()
    mock_fs.ls.return_value = []

    with patch("fsspec.filesystem", return_value=mock_fs):
        list(get_pubmed_baseline(config=test_config))
        mock_fs.ls.assert_called_once_with("/mock/no_slash/")
