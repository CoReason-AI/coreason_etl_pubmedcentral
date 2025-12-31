# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import pytest

from coreason_etl_pubmedcentral.manifest import parse_manifest


def test_manifest_complex_quoting() -> None:
    """
    Test CSV parsing with escaped quotes and commas.
    Row: "oa_comm/xml/PMC""Quote"".xml", PMC1, ...
    """
    data = [
        "Header",
        '"oa_comm/xml/PMC""Quote"".xml",PMC1,2024-01-01 12:00:00,123,CC0,no',
    ]
    records = list(parse_manifest(iter(data)))
    assert len(records) == 1
    # Standard CSV quoting: "" -> "
    assert records[0].file_path == 'oa_comm/xml/PMC"Quote".xml'


def test_manifest_complex_newlines() -> None:
    """
    Test CSV parsing with newlines inside quoted fields.
    """
    # Note: the iterator passed to csv.reader usually yields lines.
    # If a field has a newline, csv.reader might expect the iterator to yield the full line?
    # Actually, csv.reader iterates the input. If input is a list of strings (lines),
    # and a quoted field spans lines, csv.reader handles it ONLY if the input iterator preserves structure?
    # Wait, if I pass a list of strings ["line1", "line2"], and line1 ends inside a quote...
    # csv.reader works on an iterator of strings.
    # Let's see if it handles split lines.
    data = [
        "Header",
        '"oa_comm/xml/PMC\nNewline.xml",PMC1,2024-01-01 12:00:00,123,CC0,no',
    ]
    # If this is passed as a single string in the list, it's one "row".
    records = list(parse_manifest(iter(data)))
    assert len(records) == 1
    assert records[0].file_path == "oa_comm/xml/PMC\nNewline.xml"


def test_manifest_complex_bom_handling() -> None:
    """
    Test handling of UTF-8 BOM at the start of the file.
    If not handled, the first header or data might be corrupted.
    Our parser skips the first row (Header).
    If BOM is present, row[0] of header is '\ufeffFile Path'.
    We skip it.
    Row 2 is data. "path...".
    So technically BOM on header row is ignored by our 'skip header' logic?
    Let's verify.
    """
    # Simulate lines from a file opened with utf-8 (not utf-8-sig)
    data = [
        "\ufeffFile Path,Accession ID,Last Updated,PMID,License,Retracted",
        "path.xml,PMC1,2024-01-01 12:00:00,123,CC0,no",
    ]
    records = list(parse_manifest(iter(data)))
    assert len(records) == 1
    assert records[0].file_path == "path.xml"
    assert records[0].accession_id == "PMC1"

    # What if NO header? (We assume header).
    # If the file has NO header but starts with BOM?
    # parser.py: _ = next(reader) -> skips first row.
    # So if data is row 1, we lose it.
    # But spec implies header.


def test_manifest_complex_mixed_delimiters() -> None:
    """
    Test robustness against mixed delimiters if any (Standard CSV uses comma).
    We expect strictly comma.
    If tab separated?
    """
    data = [
        "Header",
        "path.xml\tPMC1\t2024-01-01 12:00:00\t123\tCC0\tno",
    ]
    # Should fail to parse as 6 columns
    records = list(parse_manifest(iter(data)))
    # Row len will be 1.
    # parser logic: if len(row) < 6: continue
    assert len(records) == 0


def test_manifest_complex_garbage_rows() -> None:
    """
    Test handling of garbage/incomplete rows interspersed with valid data.
    """
    data = [
        "Header",
        "Valid1,PMC1,2024-01-01 12:00:00,123,CC0,no",
        "Garbage row with no commas",
        "Partial,Row,2024",
        "Valid2,PMC2,2024-01-01 12:00:00,123,CC0,no",
    ]
    records = list(parse_manifest(iter(data)))
    assert len(records) == 2
    assert records[0].file_path == "Valid1"
    assert records[1].file_path == "Valid2"
