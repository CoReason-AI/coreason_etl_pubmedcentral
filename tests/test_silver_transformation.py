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
import tempfile
from unittest import mock

import pytest

from coreason_etl_pubmedcentral.silver_transformation import _silver_transformer_generator, parse_pmc_xml
from coreason_etl_pubmedcentral.tarball_processor import ParsedArticleState
from coreason_etl_pubmedcentral.xml_parser import (
    ArticleEntityState,
    ArticleFundingState,
    ArticleIdentityState,
    ArticleTemporalState,
    ArticleTypeEnum,
)


@pytest.fixture
def mock_parsed_article() -> ParsedArticleState:
    return ParsedArticleState(
        identity=ArticleIdentityState(
            pmcid="12345", pmid="54321", doi="10.1234/5678", article_type=ArticleTypeEnum.RESEARCH
        ),
        temporal=ArticleTemporalState(
            date_published="2024-01-01", date_received="2023-12-01", date_accepted="2023-12-15"
        ),
        entity=ArticleEntityState(contributors=[]),
        funding=ArticleFundingState(funding=[]),
    )


@pytest.fixture
def mock_parsed_article_no_pmcid() -> ParsedArticleState:
    return ParsedArticleState(
        identity=ArticleIdentityState(
            pmcid="", pmid="54321", doi="10.1234/5678", article_type=ArticleTypeEnum.RESEARCH
        ),
        temporal=ArticleTemporalState(
            date_published="2024-01-01", date_received="2023-12-01", date_accepted="2023-12-15"
        ),
        entity=ArticleEntityState(contributors=[]),
        funding=ArticleFundingState(funding=[]),
    )


@mock.patch("coreason_etl_pubmedcentral.silver_transformation.stream_tarball_xmls")
def test_parse_pmc_xml_success(mock_stream: mock.MagicMock, mock_parsed_article: ParsedArticleState) -> None:
    """Positive test verifying successful transformation of a bronze record into a silver record."""
    mock_stream.return_value = [mock_parsed_article]

    fd, path = tempfile.mkstemp(suffix=".tar.gz")
    os.close(fd)

    bronze_item = {"source_file_path": path}

    generator = _silver_transformer_generator(bronze_item)
    records = list(generator)

    os.remove(path)

    assert len(records) == 1
    record = records[0]

    assert "coreason_id" in record
    assert record["pmcid"] == "12345"
    assert record["source_file_path"] == path
    assert "identity" in record["article"]


@mock.patch("coreason_etl_pubmedcentral.silver_transformation.stream_tarball_xmls")
def test_parse_pmc_xml_missing_source_path(mock_stream: mock.MagicMock) -> None:
    """Negative test verifying handling of bronze record missing source_file_path."""
    bronze_item = {"other_key": "value"}

    generator = _silver_transformer_generator(bronze_item)
    records = list(generator)

    assert len(records) == 0
    mock_stream.assert_not_called()


def test_parse_pmc_xml_file_not_found() -> None:
    """Negative test verifying handling of a missing file."""
    bronze_item = {"source_file_path": "/path/to/nonexistent/file.tar.gz"}

    generator = _silver_transformer_generator(bronze_item)
    records = list(generator)

    assert len(records) == 0


@mock.patch("coreason_etl_pubmedcentral.silver_transformation.stream_tarball_xmls")
def test_parse_pmc_xml_unexpected_error(mock_stream: mock.MagicMock) -> None:
    """Negative test verifying handling of unexpected streaming errors."""
    mock_stream.side_effect = Exception("Unexpected failure")

    fd, path = tempfile.mkstemp(suffix=".tar.gz")
    os.close(fd)

    bronze_item = {"source_file_path": path}

    generator = _silver_transformer_generator(bronze_item)
    records = list(generator)

    os.remove(path)

    assert len(records) == 0


@mock.patch("coreason_etl_pubmedcentral.silver_transformation.stream_tarball_xmls")
def test_parse_pmc_xml_missing_pmcid(
    mock_stream: mock.MagicMock, mock_parsed_article_no_pmcid: ParsedArticleState
) -> None:
    """Negative test verifying skipping of records with missing PMCID."""
    mock_stream.return_value = [mock_parsed_article_no_pmcid]

    fd, path = tempfile.mkstemp(suffix=".tar.gz")
    os.close(fd)

    bronze_item = {"source_file_path": path}

    generator = _silver_transformer_generator(bronze_item)
    records = list(generator)

    os.remove(path)

    assert len(records) == 0


@mock.patch("coreason_etl_pubmedcentral.silver_transformation.generate_surrogate_keys")
@mock.patch("coreason_etl_pubmedcentral.silver_transformation.stream_tarball_xmls")
def test_parse_pmc_xml_failed_surrogate_key(
    mock_stream: mock.MagicMock, mock_surrogate_keys: mock.MagicMock, mock_parsed_article: ParsedArticleState
) -> None:
    """Negative test verifying skipping of records when surrogate key generation fails."""
    mock_stream.return_value = [mock_parsed_article]
    mock_surrogate_keys.return_value = [None]  # Simulate generation failure

    fd, path = tempfile.mkstemp(suffix=".tar.gz")
    os.close(fd)

    bronze_item = {"source_file_path": path}

    generator = _silver_transformer_generator(bronze_item)
    records = list(generator)

    os.remove(path)

    assert len(records) == 0


def test_parse_pmc_xml_decorator() -> None:
    """Test the dlt decorator wrapper initialization."""
    # Since dlt wrappers don't easily allow bypassing the engine to call _func,
    # we just assert it's a valid DltResource object with the correct attributes
    assert parse_pmc_xml.name == "silver_pmc_article"
    assert parse_pmc_xml.write_disposition == "append"
