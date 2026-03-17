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
import os
import tempfile
from collections.abc import Generator
from unittest import mock

import duckdb
import pytest

from coreason_etl_pubmedcentral.manifest_parser import parse_manifest

# Mock CSV content matching standard PMC format
# File,Article Citation,Accession ID,Last Updated (YYYY-MM-DD HH:MM:SS),PMID,License,Retracted
MOCK_CSV_HEADER = "File,Article Citation,Accession ID,Last Updated (YYYY-MM-DD HH:MM:SS),PMID,License,Retracted\n"

MOCK_CSV_DATA = """oa_comm/xml/all/PMC1234567.xml.tar.gz,Some Article 1,PMC1234567,2024-01-01 12:00:00,12345,CC-BY,no
oa_comm/xml/all/PMC2345678.xml.tar.gz,Some Article 2,PMC2345678,2024-01-02 12:00:00,23456,CC0,yes
oa_comm/xml/all/PMC3456789.xml.tar.gz,Some Article 3,PMC3456789,2024-01-03 12:00:00,34567,CC-BY-NC,NO
"""


@pytest.fixture
def mock_manifest_file() -> Generator[str]:
    """Fixture that creates a temporary mock manifest CSV file."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, "w") as f:
        f.write(MOCK_CSV_HEADER + MOCK_CSV_DATA)
    yield path
    os.remove(path)


@pytest.fixture
def mock_empty_manifest_file() -> Generator[str]:
    """Fixture that creates a temporary empty manifest CSV file (only headers)."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, "w") as f:
        f.write(MOCK_CSV_HEADER)
    yield path
    os.remove(path)


def test_parse_manifest_no_watermark(mock_manifest_file: str) -> None:
    """Positive test verifying all records are returned when no watermark is provided."""
    records = list(parse_manifest(mock_manifest_file))

    assert len(records) == 3

    # Check first record
    assert records[0]["file_path"] == "oa_comm/xml/all/PMC1234567.xml.tar.gz"
    assert records[0]["accession_id"] == "PMC1234567"
    assert records[0]["last_updated"] == "2024-01-01 12:00:00"
    assert records[0]["pmid"] == 12345
    assert records[0]["license"] == "CC-BY"
    assert records[0]["retracted"] is False

    # Check second record (retracted)
    assert records[1]["accession_id"] == "PMC2345678"
    assert records[1]["retracted"] is True

    # Check third record (not retracted, case-insensitive check)
    assert records[2]["accession_id"] == "PMC3456789"
    assert records[2]["retracted"] is False


def test_parse_manifest_with_watermark(mock_manifest_file: str) -> None:
    """Test verifying High-Water Mark filtering logic."""
    # Set watermark to 2024-01-02 00:00:00, should return 2 records (Jan 2 and Jan 3)
    watermark = datetime.datetime(2024, 1, 2, 0, 0, 0)
    records = list(parse_manifest(mock_manifest_file, last_updated_watermark=watermark))

    assert len(records) == 2
    assert records[0]["accession_id"] == "PMC2345678"
    assert records[1]["accession_id"] == "PMC3456789"

    # Set watermark to 2024-01-03 12:00:00, should return 0 records
    watermark_future = datetime.datetime(2024, 1, 3, 12, 0, 0)
    records_future = list(parse_manifest(mock_manifest_file, last_updated_watermark=watermark_future))

    assert len(records_future) == 0


def test_parse_manifest_empty_file(mock_empty_manifest_file: str) -> None:
    """Boundary test handling an empty manifest file."""
    records = list(parse_manifest(mock_empty_manifest_file))
    assert len(records) == 0


def test_parse_manifest_duckdb_error() -> None:
    """Negative test verifying handling of DuckDB query execution errors (e.g., file not found)."""
    with pytest.raises(duckdb.Error):
        list(parse_manifest("non_existent_file.csv"))


def test_parse_manifest_boolean_retracted() -> None:
    """Test when DuckDB auto-detects 'Retracted' as a boolean type."""
    # Write a quick temp file with proper headers and 'true'/'false' values
    fd, path = tempfile.mkstemp(suffix=".csv")
    content = MOCK_CSV_HEADER + "file1,cite,PMC1,2024-01-01 12:00:00,123,CC-BY,true\n"
    content += "file2,cite,PMC2,2024-01-01 12:00:00,123,CC-BY,false\n"
    with os.fdopen(fd, "w") as f:
        f.write(content)

    records = list(parse_manifest(path))
    os.remove(path)

    assert len(records) == 2
    assert records[0]["retracted"] is True
    assert records[1]["retracted"] is False


@mock.patch("duckdb.connect")
def test_parse_manifest_duckdb_mock_error(mock_connect: mock.MagicMock) -> None:
    """Negative test verifying handling of DuckDB query execution errors from DB level."""
    mock_conn = mock.MagicMock()
    mock_conn.execute.side_effect = duckdb.Error("Mock error")
    mock_connect.return_value.__enter__.return_value = mock_conn

    with pytest.raises(duckdb.Error):
        list(parse_manifest("fake_file.csv"))


def test_parse_manifest_none_retracted() -> None:
    """Test when DuckDB returns None or empty string for retracted flag."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    content = MOCK_CSV_HEADER + "file1,cite,PMC1,2024-01-01 12:00:00,123,CC-BY,\n"
    with os.fdopen(fd, "w") as f:
        f.write(content)

    records = list(parse_manifest(path))
    os.remove(path)

    assert len(records) == 1
    assert records[0]["retracted"] is False


def test_parse_manifest_non_yes_retracted() -> None:
    """Test when DuckDB returns a string for retracted flag that is not 'yes'."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    content = MOCK_CSV_HEADER + "file1,cite,PMC1,2024-01-01 12:00:00,123,CC-BY,unknown\n"
    with os.fdopen(fd, "w") as f:
        f.write(content)

    records = list(parse_manifest(path))
    os.remove(path)

    assert len(records) == 1
    assert records[0]["retracted"] is False


def test_parse_manifest_str_yes_retracted() -> None:
    """Test when DuckDB returns a string for retracted flag that is 'yes' but with different case or space."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    content = MOCK_CSV_HEADER + "file1,cite,PMC1,2024-01-01 12:00:00,123,CC-BY, YES \n"
    with os.fdopen(fd, "w") as f:
        f.write(content)

    records = list(parse_manifest(path))
    os.remove(path)

    assert len(records) == 1
    assert records[0]["retracted"] is True
