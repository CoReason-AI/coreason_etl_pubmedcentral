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
from collections.abc import Iterator

import pytest

from coreason_etl_pubmedcentral.models.manifest_parser import EpistemicManifestParsingTask


@pytest.fixture
def mock_csv_stream() -> Iterator[str]:
    """Provides a mocked PMC filelist CSV stream."""
    data = [
        "File Path,Accession ID,Last Updated (UTC),PMID,License,Retracted",
        "oa_comm/xml/all/PMC123.xml.tar.gz,PMC123,2024-03-25 15:42:01,1001,CC BY,no",
        "oa_comm/xml/all/PMC124.xml.tar.gz,PMC124,2024-03-26 10:00:00,1002,CC0,yes",
        "oa_comm/xml/all/PMC125.xml.tar.gz,PMC125,2024-03-27 12:30:00,,CC BY-NC,no",
        "oa_comm/xml/all/PMC126.xml.tar.gz,PMC126,2024-03-27 12:30:00,1004,CC BY,yes",
    ]
    return iter(data)


@pytest.fixture
def malformed_csv_stream() -> Iterator[str]:
    """Provides a mocked PMC filelist CSV stream with malformed or missing data."""
    data = [
        "File Path,Accession ID,Last Updated (UTC),PMID,License,Retracted",
        "oa_comm/xml/all/PMC123.xml.tar.gz,PMC123,INVALID DATE,1001,CC BY,no",
        ",PMC124,2024-03-26 10:00:00,1002,CC0,yes",  # Missing File Path
        "oa_comm/xml/all/PMC125.xml.tar.gz,PMC125,,1003,CC BY-NC,no",  # Missing Date
    ]
    return iter(data)


@pytest.fixture
def empty_csv_stream() -> Iterator[str]:
    """Provides a totally empty file stream."""
    return iter([])


def test_epistemic_manifest_parsing_task_all_records(mock_csv_stream: Iterator[str]) -> None:
    """Test that all valid records are parsed when no high-water mark is provided."""
    results = list(EpistemicManifestParsingTask.execute(mock_csv_stream))

    assert len(results) == 4

    # Check proper retraction parsing
    assert results[0]["is_retracted"] is False
    assert results[1]["is_retracted"] is True

    # Check null handling
    assert results[0]["pmid"] == "1001"
    assert results[2]["pmid"] is None

    # Check timezone awareness
    assert results[0]["last_updated"].tzinfo == datetime.UTC


def test_epistemic_manifest_parsing_task_high_water_mark(mock_csv_stream: Iterator[str]) -> None:
    """Test that the High-Water Mark filters out older records securely."""
    # Target High-Water Mark exactly at PMC124's timestamp
    water_mark = datetime.datetime(2024, 3, 26, 10, 0, 0, tzinfo=datetime.UTC)

    results = list(EpistemicManifestParsingTask.execute(mock_csv_stream, water_mark))

    # Only records AFTER the water_mark should remain
    assert len(results) == 2

    assert results[0]["accession_id"] == "PMC125"
    assert results[1]["accession_id"] == "PMC126"


def test_epistemic_manifest_parsing_task_malformed_data(malformed_csv_stream: Iterator[str]) -> None:
    """Test that the parser safely skips completely malformed records without raising exceptions."""
    results = list(EpistemicManifestParsingTask.execute(malformed_csv_stream))

    # All rows in the malformed stream have critical issues, so we expect empty results
    assert len(results) == 0


def test_epistemic_manifest_parsing_task_empty_stream(empty_csv_stream: Iterator[str]) -> None:
    """Test that the parser handles an empty stream (no headers) gracefully."""
    results = list(EpistemicManifestParsingTask.execute(empty_csv_stream))
    assert len(results) == 0
