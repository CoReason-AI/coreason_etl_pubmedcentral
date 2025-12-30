# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from typing import Any
from unittest.mock import patch

from coreason_etl_pubmedcentral.pipeline_gold import _pmc_gold_generator, transform_gold_record


def test_gold_transformation_full_record() -> None:
    """Test transformation of a fully populated Silver record."""
    silver_record: dict[str, Any] = {
        "pmcid": "12345",
        "pmid": "9999",
        "doi": "10.1000/xyz",
        "article_type": "RESEARCH",
        "date_published": "2024-03-15",
        "date_received": "2024-01-01",
        "date_accepted": "2024-02-01",
        "title": "Test Article",
        "abstract": "This is a test abstract.",
        "journal_name": "Journal of Testing",
        "keywords": ["test", "etl"],
        "authors": [
            {"surname": "Doe", "given_names": "John", "affiliations": ["University A"]},
            {"surname": "Smith", "given_names": "Jane", "affiliations": ["University A", "Institute B"]},
        ],
        "funding": [
            {"agency": "NIH", "grant_id": "G1"},
            {"agency": "NSF", "grant_id": "G2"},
            {"agency": "NIH", "grant_id": "G3"},  # Duplicate agency
        ],
        "is_retracted": True,
        "manifest_metadata": {"license_type": "CC-BY", "last_updated": "2024-04-01T12:00:00"},
        "ingestion_metadata": {
            "source_file_path": "oa_comm/xml/PMC12345.xml",
            "ingestion_ts": "2024-04-01T12:00:00",
            "ingestion_source": "S3",
        },
    }

    gold = transform_gold_record(silver_record)
    assert gold is not None

    # Check Compliance
    assert gold["is_commercial_safe"] is True
    assert gold["is_retracted"] is True
    assert gold["license_type"] == "CC-BY"

    # Check Context
    assert gold["journal_name"] == "Journal of Testing"
    assert gold["pub_year"] == 2024

    # Check Search
    assert gold["title"] == "Test Article"
    assert gold["abstract"] == "This is a test abstract."
    assert gold["authors_display"] == "Doe John; Smith Jane"

    # Check Filters (Sets/Lists)
    assert set(gold["keywords"]) == {"test", "etl"}
    assert set(gold["affiliations_text"]) == {"University A", "Institute B"}
    assert set(gold["agency_names"]) == {"NIH", "NSF"}
    assert set(gold["grant_ids"]) == {"G1", "G2", "G3"}

    # Check Keys
    assert gold["pmcid"] == "12345"


def test_gold_commercial_safe_logic() -> None:
    """Test is_commercial_safe logic based on file path."""

    # Case 1: oa_comm
    rec1 = {"ingestion_metadata": {"source_file_path": "oa_comm/xml/file.xml"}}
    result1 = transform_gold_record(rec1)
    assert result1 is not None
    assert result1["is_commercial_safe"] is True

    # Case 2: oa_noncomm
    rec2 = {"ingestion_metadata": {"source_file_path": "oa_noncomm/xml/file.xml"}}
    result2 = transform_gold_record(rec2)
    assert result2 is not None
    assert result2["is_commercial_safe"] is False

    # Case 3: Unknown/Other
    rec3 = {"ingestion_metadata": {"source_file_path": "random/path.xml"}}
    result3 = transform_gold_record(rec3)
    assert result3 is not None
    assert result3["is_commercial_safe"] is False


def test_gold_date_parsing() -> None:
    """Test publication year extraction."""

    # Case 1: Valid YYYY-MM-DD
    rec1 = {"date_published": "2023-12-31"}
    result1 = transform_gold_record(rec1)
    assert result1 is not None
    assert result1["pub_year"] == 2023

    # Case 2: Missing Date
    rec2: dict[str, Any] = {}
    result2 = transform_gold_record(rec2)
    assert result2 is not None
    assert result2["pub_year"] is None

    # Case 3: Invalid Date Format
    rec3 = {"date_published": "invalid-date"}
    result3 = transform_gold_record(rec3)
    assert result3 is not None
    assert result3["pub_year"] is None

    # Case 4: None
    rec4 = {"date_published": None}
    result4 = transform_gold_record(rec4)
    assert result4 is not None
    assert result4["pub_year"] is None


def test_gold_authors_affiliations() -> None:
    """Test author string formatting and affiliation deduplication."""

    rec = {
        "authors": [
            {"surname": "A", "given_names": "B", "affiliations": ["U1"]},
            {"surname": "C", "given_names": None, "affiliations": ["U1", "U2"]},
            {"surname": None, "given_names": "D", "affiliations": []},
            {"surname": None, "given_names": None, "affiliations": ["U3"]},  # No name, just aff
        ]
    }

    gold = transform_gold_record(rec)
    assert gold is not None

    # Authors display: "A B; C; D"
    assert gold["authors_display"] == "A B; C; D"

    # Affiliations: U1, U2, U3
    assert set(gold["affiliations_text"]) == {"U1", "U2", "U3"}


def test_gold_funding_aggregation() -> None:
    """Test funding aggregation."""
    rec = {
        "funding": [
            {"agency": "A1", "grant_id": None},
            {"agency": None, "grant_id": "G1"},
            {"agency": "A1", "grant_id": "G2"},
        ]
    }

    gold = transform_gold_record(rec)
    assert gold is not None

    assert set(gold["agency_names"]) == {"A1"}
    assert set(gold["grant_ids"]) == {"G1", "G2"}


def test_gold_minimal_record() -> None:
    """Test transformation of a minimal record with missing optional fields."""
    rec: dict[str, Any] = {"pmcid": "123"}

    gold = transform_gold_record(rec)
    assert gold is not None

    assert gold["pmcid"] == "123"
    assert gold["pub_year"] is None
    assert gold["grant_ids"] == []
    assert gold["keywords"] == []
    assert gold["is_commercial_safe"] is False  # Default


def test_gold_generator() -> None:
    """Test the generator function for success and error handling."""

    # Mock data
    items = [
        {"pmcid": "1", "title": "OK 1"},
        {"pmcid": "2", "title": "Error"},  # Will trigger error
        {"pmcid": "3", "title": "OK 3"},
    ]

    # We patch transform_gold_record to raise exception for item 2
    with patch("coreason_etl_pubmedcentral.pipeline_gold.transform_gold_record") as mock_transform:

        def side_effect(item: dict[str, Any]) -> dict[str, Any]:
            if item["pmcid"] == "2":
                raise ValueError("Bang!")
            return {"pmcid": item["pmcid"], "title": item["title"], "transformed": True}

        mock_transform.side_effect = side_effect

        # Run generator
        results = list(_pmc_gold_generator(items))  # type: ignore

        # Verify results: should have 1 and 3, skipped 2
        assert len(results) == 2
        assert results[0]["pmcid"] == "1"
        assert results[1]["pmcid"] == "3"

        # Verify logs? (Optional, requires capturing logs)
        # Verify calls
        assert mock_transform.call_count == 3


def test_gold_complex_unicode_and_special_chars() -> None:
    """Test transformation with Unicode and special characters."""
    rec: dict[str, Any] = {
        "pmcid": "123",
        "title": "Title with üñîçødę",
        "authors": [
            {"surname": "Müller", "given_names": "Jürgen", "affiliations": ["Universität München"]},
            {"surname": "O'Connor", "given_names": "Renée", "affiliations": []},
        ],
        "funding": [{"agency": "Fünding Agency €", "grant_id": "GR#123&456"}],
    }

    gold = transform_gold_record(rec)
    assert gold is not None

    assert gold["title"] == "Title with üñîçødę"
    assert "Müller Jürgen" in gold["authors_display"]
    assert "O'Connor Renée" in gold["authors_display"]
    assert "Universität München" in gold["affiliations_text"]
    assert "Fünding Agency €" in gold["agency_names"]
    assert "GR#123&456" in gold["grant_ids"]


def test_gold_deduplication_logic() -> None:
    """Verify strictly that duplicates are removed."""
    rec: dict[str, Any] = {
        "pmcid": "123",
        "authors": [
            {"surname": "A", "given_names": "B", "affiliations": ["U1", "U1", "U2"]},  # Duplicate in list
            {"surname": "C", "given_names": "D", "affiliations": ["U2", "U3"]},  # Overlap
        ],
        "funding": [
            {"agency": "A1", "grant_id": "G1"},
            {"agency": "A1", "grant_id": "G1"},  # Exact Duplicate
            {"agency": "A2", "grant_id": "G1"},  # Same grant, diff agency
        ],
    }

    gold = transform_gold_record(rec)
    assert gold is not None

    # U1, U2, U3 -> 3 unique
    assert len(gold["affiliations_text"]) == 3
    assert set(gold["affiliations_text"]) == {"U1", "U2", "U3"}

    # Agencies: A1, A2
    assert set(gold["agency_names"]) == {"A1", "A2"}
    # Grants: G1
    assert set(gold["grant_ids"]) == {"G1"}


def test_gold_date_edge_cases() -> None:
    """Test robust date parsing edge cases."""
    # 1. Just Year
    rec1 = {"date_published": "2024"}
    res1 = transform_gold_record(rec1)
    assert res1["pub_year"] == 2024  # type: ignore

    # 2. Year Month
    rec2 = {"date_published": "2024-05"}
    res2 = transform_gold_record(rec2)
    assert res2["pub_year"] == 2024  # type: ignore

    # 3. Full ISO
    rec3 = {"date_published": "2024-05-15T12:00:00"}
    res3 = transform_gold_record(rec3)
    assert res3["pub_year"] == 2024  # type: ignore

    # 4. Bad String
    rec4 = {"date_published": "NotADate"}
    res4 = transform_gold_record(rec4)
    assert res4["pub_year"] is None  # type: ignore


def test_gold_robust_null_handling() -> None:
    """Test robust handling of partial or null objects in lists."""
    rec: dict[str, Any] = {
        "pmcid": "123",
        "authors": [
            {"surname": None, "given_names": None, "affiliations": [None, ""]},  # Should be ignored/empty
            {"surname": "Valid", "given_names": None, "affiliations": ["U1"]},
        ],
        "funding": [
            {"agency": None, "grant_id": None},  # Should be ignored
            {"agency": "", "grant_id": ""},  # Should be ignored
            {"agency": "A1", "grant_id": None},
        ],
        "keywords": [None, ""],  # If keywords allows this? logic is simple pass through usually
    }

    gold = transform_gold_record(rec)
    assert gold is not None

    # Authors display should just be "Valid"
    assert gold["authors_display"] == "Valid"
    # Affiliations: U1 (None and "" ignored)
    assert gold["affiliations_text"] == ["U1"]
    # Agencies: A1
    assert gold["agency_names"] == ["A1"]
    # Grant IDs: Empty
    assert gold["grant_ids"] == []
