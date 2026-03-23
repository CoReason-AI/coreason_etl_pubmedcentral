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
from coreason_etl_pubmedabstracts.resources.updates import get_pubmed_updates


@pytest.fixture
def updates_config() -> PubMedAbstractsConfig:
    """Fixture providing an updates configuration."""
    config = PubMedAbstractsConfig()
    config.ftp_host = "ftp.mock.org"
    config.updates_dir = "/mock/updatefiles/"
    return config


def test_get_pubmed_updates_success(updates_config: PubMedAbstractsConfig) -> None:
    """Test the successful extraction and strict sorting of delta files from FTP."""
    xml_data = b"""<?xml version="1.0" ?>
    <PubmedArticleSet>
        <DeleteCitation>
            <PMID>999</PMID>
        </DeleteCitation>
    </PubmedArticleSet>
    """
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="w") as f:
        f.write(xml_data)

    mock_gz_stream = io.BytesIO(out.getvalue())

    mock_fs = MagicMock()
    # List is intentionally out of order to ensure the `sorted()` logic works
    mock_fs.ls.return_value = [
        "/mock/updatefiles/update26n0003.xml.gz",
        "/mock/updatefiles/update26n0001.xml.gz",
        "/mock/updatefiles/update26n0002.xml.gz",
        "/mock/updatefiles/ignore.txt",
    ]

    def mock_open_side_effect(*_args: Any, **_kwargs: Any) -> MagicMock:
        mock_gz_stream.seek(0)
        return MagicMock(__enter__=lambda _: mock_gz_stream, __exit__=lambda *_x: None)

    mock_fs.open.side_effect = mock_open_side_effect

    with patch("fsspec.filesystem", return_value=mock_fs):
        resource_generator = get_pubmed_updates(config=updates_config)
        results = list(resource_generator)

        # Expecting 3 valid xml.gz files, sorted
        assert len(results) == 3

        # Test Alphanumeric sort enforcement
        assert results[0]["file_name"] == "update26n0001.xml.gz"
        assert results[1]["file_name"] == "update26n0002.xml.gz"
        assert results[2]["file_name"] == "update26n0003.xml.gz"

        # Verify schema mapping and extraction
        assert "ingestion_ts" in results[0]
        assert "content_hash" in results[0]
        assert results[0]["raw_data"]["PMID"] == "999"


def test_get_pubmed_updates_fs_failure(updates_config: PubMedAbstractsConfig) -> None:
    """Test graceful fallback for empty or failed directory list on updates."""
    mock_fs = MagicMock()
    mock_fs.ls.side_effect = Exception("FTP Timeout")

    with patch("fsspec.filesystem", return_value=mock_fs):
        results = list(get_pubmed_updates(config=updates_config))
        assert len(results) == 0


def test_get_pubmed_updates_no_trailing_slash(updates_config: PubMedAbstractsConfig) -> None:
    """Test adding trailing slash to directory paths."""
    updates_config.updates_dir = "/mock/updates_no_slash"

    mock_fs = MagicMock()
    mock_fs.ls.return_value = []

    with patch("fsspec.filesystem", return_value=mock_fs):
        list(get_pubmed_updates(config=updates_config))
        mock_fs.ls.assert_called_once_with("/mock/updates_no_slash/")
