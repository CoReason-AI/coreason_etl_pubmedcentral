# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from datetime import datetime, timezone
from typing import Iterator

import pytest

from coreason_etl_pubmedcentral.manifest import parse_manifest


@pytest.fixture  # type: ignore
def valid_csv_lines() -> Iterator[str]:
    data = [
        "File Path,Accession ID,Last Updated (UTC),PMID,License,Retracted",
        "oa_comm/xml/PMC1.xml,PMC1,2024-01-01 10:00:00,1001,CC-BY,no",
        "oa_comm/xml/PMC2.xml,PMC2,2024-01-02 10:00:00,1002,CC0,yes",
        "oa_comm/xml/PMC3.xml,PMC3,2024-01-03 10:00:00,,CC-BY,no",
    ]
    return iter(data)


def test_parse_manifest_all(valid_csv_lines: Iterator[str]) -> None:
    records = list(parse_manifest(valid_csv_lines))
    assert len(records) == 3

    # Check Record 1
    r1 = records[0]
    assert r1.file_path == "oa_comm/xml/PMC1.xml"
    assert r1.accession_id == "PMC1"
    assert r1.last_updated == datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    assert r1.pmid == "1001"
    assert r1.license_type == "CC-BY"
    assert r1.is_retracted is False

    # Check Record 2 (Retracted)
    r2 = records[1]
    assert r2.accession_id == "PMC2"
    assert r2.is_retracted is True

    # Check Record 3 (No PMID)
    r3 = records[2]
    assert r3.pmid is None


def test_parse_manifest_cutoff(valid_csv_lines: Iterator[str]) -> None:
    cutoff = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    records = list(parse_manifest(valid_csv_lines, last_ingested_cutoff=cutoff))

    # Should exclude first record (10:00 < 12:00)
    # Should include 2nd (Jan 2) and 3rd (Jan 3)
    assert len(records) == 2
    assert records[0].accession_id == "PMC2"
    assert records[1].accession_id == "PMC3"


def test_parse_manifest_exact_cutoff(valid_csv_lines: Iterator[str]) -> None:
    cutoff = datetime(2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc)
    records = list(parse_manifest(valid_csv_lines, last_ingested_cutoff=cutoff))

    # Should exclude 1st and 2nd (<= cutoff)
    assert len(records) == 1
    assert records[0].accession_id == "PMC3"


def test_parse_manifest_invalid_date() -> None:
    data = [
        "Header",
        "path,acc,INVALID_DATE,pmid,lic,no",
        "path2,acc2,2024-01-01 10:00:00,pmid,lic,no",
    ]
    records = list(parse_manifest(iter(data)))
    # First row skipped, second parsed
    assert len(records) == 1
    assert records[0].accession_id == "acc2"


def test_parse_manifest_short_row() -> None:
    data = [
        "Header",
        "path,acc,2024-01-01 10:00:00",  # Missing columns
        "path2,acc2,2024-01-01 10:00:00,pmid,lic,no",
    ]
    records = list(parse_manifest(iter(data)))
    assert len(records) == 1
    assert records[0].accession_id == "acc2"


def test_parse_manifest_empty() -> None:
    records = list(parse_manifest(iter([])))
    assert len(records) == 0


def test_parse_manifest_header_only() -> None:
    records = list(parse_manifest(iter(["Header"])))
    assert len(records) == 0


def test_parse_manifest_blank_lines() -> None:
    # Test handling of blank lines in CSV
    data = [
        "Header",
        "",  # Blank line
        "oa_comm/xml/PMC1.xml,PMC1,2024-01-01 10:00:00,1001,CC-BY,no",
        "",  # Blank line
    ]
    records = list(parse_manifest(iter(data)))
    assert len(records) == 1
    assert records[0].accession_id == "PMC1"


def test_parse_manifest_quoted_fields() -> None:
    # Test fields containing commas within quotes
    data = [
        "Header",
        '"path/to/file,with,commas.xml",PMC_Q,2024-01-01 10:00:00,1234,"CC,BY",no',
    ]
    records = list(parse_manifest(iter(data)))
    assert len(records) == 1
    assert records[0].file_path == "path/to/file,with,commas.xml"
    assert records[0].license_type == "CC,BY"


def test_parse_manifest_whitespace_stripping() -> None:
    # Test surrounding whitespace is stripped
    data = [
        "Header",
        "  path/xml  ,  PMC_WS  ,  2024-01-01 10:00:00  ,  1234  ,  CC-BY  ,  no  ",
    ]
    records = list(parse_manifest(iter(data)))
    assert len(records) == 1
    assert records[0].file_path == "path/xml"
    assert records[0].accession_id == "PMC_WS"
    # Date parsing should handle whitespace because we strip before parsing
    assert records[0].last_updated == datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    assert records[0].pmid == "1234"
    assert records[0].license_type == "CC-BY"
    assert records[0].is_retracted is False


def test_parse_manifest_retracted_case_insensitivity() -> None:
    # Test variants of "yes"
    data = [
        "Header",
        "p1,acc1,2024-01-01 10:00:00,,Lic,YES",
        "p2,acc2,2024-01-01 10:00:00,,Lic,Yes",
        "p3,acc3,2024-01-01 10:00:00,,Lic,yes",
        "p4,acc4,2024-01-01 10:00:00,,Lic,NO",
    ]
    records = list(parse_manifest(iter(data)))
    assert len(records) == 4
    assert records[0].is_retracted is True
    assert records[1].is_retracted is True
    assert records[2].is_retracted is True
    assert records[3].is_retracted is False


def test_parse_manifest_unicode_handling() -> None:
    # Test Unicode characters in fields
    data = [
        "Header",
        "path/αβγ.xml,PMC_Ω,2024-01-01 10:00:00,1234,CC-©,no",
    ]
    records = list(parse_manifest(iter(data)))
    assert len(records) == 1
    assert records[0].file_path == "path/αβγ.xml"
    assert records[0].accession_id == "PMC_Ω"
    assert records[0].license_type == "CC-©"


def test_parse_manifest_timezone_forcing() -> None:
    # Ensure naive looking string becomes UTC
    data = [
        "Header",
        "p1,acc1,2024-01-01 10:00:00,1234,Lic,no",
    ]
    records = list(parse_manifest(iter(data)))
    assert len(records) == 1
    dt = records[0].last_updated
    assert dt.tzinfo == timezone.utc
    assert dt.isoformat() == "2024-01-01T10:00:00+00:00"
