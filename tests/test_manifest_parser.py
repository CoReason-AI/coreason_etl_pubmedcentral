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
    assert records[0]["pmid"] == "12345"
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


def test_parse_manifest_error() -> None:
    """Negative test verifying handling of file errors (e.g., file not found)."""
    with pytest.raises(FileNotFoundError):
        list(parse_manifest("non_existent_file.csv"))


def test_parse_manifest_none_retracted() -> None:
    """Test when retracted flag is empty."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    content = MOCK_CSV_HEADER + "file1,cite,PMC1,2024-01-01 12:00:00,123,CC-BY,\n"
    with os.fdopen(fd, "w") as f:
        f.write(content)

    records = list(parse_manifest(path))
    os.remove(path)

    assert len(records) == 1
    assert records[0]["retracted"] is False


def test_parse_manifest_non_yes_retracted() -> None:
    """Test when string for retracted flag is not 'yes'."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    content = MOCK_CSV_HEADER + "file1,cite,PMC1,2024-01-01 12:00:00,123,CC-BY,unknown\n"
    with os.fdopen(fd, "w") as f:
        f.write(content)

    records = list(parse_manifest(path))
    os.remove(path)

    assert len(records) == 1
    assert records[0]["retracted"] is False


def test_parse_manifest_str_yes_retracted() -> None:
    """Test when string for retracted flag is 'yes' but with different case or space."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    content = MOCK_CSV_HEADER + "file1,cite,PMC1,2024-01-01 12:00:00,123,CC-BY, YES \n"
    with os.fdopen(fd, "w") as f:
        f.write(content)

    records = list(parse_manifest(path))
    os.remove(path)

    assert len(records) == 1
    assert records[0]["retracted"] is True


def test_parse_manifest_malformed_date() -> None:
    """Test that malformed dates are gracefully skipped when watermark is applied."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    content = MOCK_CSV_HEADER + "file1,cite,PMC1,bad-date,123,CC-BY,no\n"
    content += "file2,cite,PMC2,2024-01-02 12:00:00,124,CC-BY,yes\n"
    with os.fdopen(fd, "w") as f:
        f.write(content)

    watermark = datetime.datetime(2024, 1, 1, 0, 0, 0)
    records = list(parse_manifest(path, last_updated_watermark=watermark))
    os.remove(path)

    assert len(records) == 1
    assert records[0]["accession_id"] == "PMC2"
    assert records[0]["retracted"] is True


def test_parse_manifest_empty_pmid() -> None:
    """Test that empty PMID results in None."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    content = MOCK_CSV_HEADER + "file1,cite,PMC1,2024-01-01 12:00:00,  ,CC-BY,no\n"
    with os.fdopen(fd, "w") as f:
        f.write(content)

    records = list(parse_manifest(path))
    os.remove(path)

    assert len(records) == 1
    assert records[0]["pmid"] is None
